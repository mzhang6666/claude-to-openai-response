use std::{
    collections::{hash_map::Entry, HashMap},
    convert::Infallible,
    fs::{File, OpenOptions},
    io::Write,
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use async_stream::stream;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{LevelFilter, Log, Metadata, Record};
use reqwest::Client;
use serde_json::{json, Map, Value};
use std::process::{Child, Command, Stdio};
use uuid::Uuid;

#[derive(Clone)]
struct Config {
    responses_url: String,
    openai_key: String,
    model: String,
    host: String,
    port: u16,
    log_file: String,
    log_level: LevelFilter,
    min_max_output_tokens: i64,
    fallback_max_output_tokens: i64,
}

#[derive(Clone)]
struct AppState {
    client: Client,
    config: Config,
}

struct MultiLogger {
    file: Mutex<Option<File>>,
    log_file: String,
    max_level: LevelFilter,
}

impl Log for MultiLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= self.max_level
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let line = format!(
            "{} {:<5} {}",
            chrono_like_timestamp(),
            record.level(),
            record.args()
        );
        eprintln!("{}", line);
        if let Ok(mut file_slot) = self.file.lock() {
            if file_slot.is_none() {
                match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.log_file)
                {
                    Ok(file) => {
                        *file_slot = Some(file);
                    }
                    Err(_) => {
                        return;
                    }
                }
            }

            if let Some(file) = file_slot.as_mut() {
                let _ = writeln!(file, "{}", line);
                let _ = file.flush();
            }
        }
    }

    fn flush(&self) {
        if let Ok(mut file_slot) = self.file.lock() {
            if let Some(file) = file_slot.as_mut() {
                let _ = file.flush();
            }
        }
    }
}

fn chrono_like_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    format!("{}.{:03}", secs, millis)
}

fn parse_log_level() -> LevelFilter {
    std::env::var("CCCTL_LOG_LEVEL")
        .ok()
        .and_then(|v| v.parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::Off)
}

fn init_logger(log_file: &str, max_level: LevelFilter) {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let logger = MultiLogger {
            file: Mutex::new(None),
            log_file: log_file.to_string(),
            max_level,
        };
        log::set_boxed_logger(Box::new(logger)).expect("failed to install logger");
        log::set_max_level(max_level);
    });
}

fn build_config() -> Config {
    Config {
        responses_url: std::env::var("OPENAI_BASE_URL").unwrap_or_default(),
        openai_key: std::env::var("OPENAI_API_KEY").unwrap_or_default(),
        model: std::env::var("OPENAI_MODEL_NAME").unwrap_or_default(),
        host: std::env::var("CCCTL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
        port: std::env::var("CCCTL_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5520),
        log_file: std::env::var("CCCTL_LOG_PATH")
            .unwrap_or_else(|_| "ccctl.log".to_string()),
        log_level: parse_log_level(),
        min_max_output_tokens: std::env::var("CCCTL_MIN_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8192),
        fallback_max_output_tokens: std::env::var("CCCTL_FALLBACK_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8192),
    }
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(root_get).head(root_head))
        .route("/v1/messages", post(proxy_messages))
        .route("/v1/models", get(list_models))
        .route("/v1/health", get(health))
        .layer(middleware::from_fn(log_http_request))
        .with_state(state)
}

async fn run_proxy_server(config: Config) -> Result<(), std::io::Error> {
    let client = Client::builder()
        .build()
        .expect("failed to build reqwest client");
    let state = AppState {
        client,
        config: config.clone(),
    };
    let app = build_app(state);
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("invalid host/port");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    axum::serve(listener, app).await
}

async fn wait_for_proxy_ready(base_url: &str, proxy_child: &mut Child) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| format!("failed to create readiness client: {}", e))?;
    let health_url = format!("{}/v1/health", base_url.trim_end_matches('/'));
    for _ in 0..100 {
        if let Ok(Some(status)) = proxy_child.try_wait() {
            return Err(format!("proxy child exited early with status {}", status));
        }
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("proxy did not become ready at {}", health_url))
}

fn spawn_proxy_child() -> Result<Child, String> {
    let exe = std::env::current_exe()
        .map_err(|e| format!("failed to resolve current executable: {}", e))?;
    let config = build_config();
    Command::new(exe)
        .arg("proxy")
        .env("CCCTL_HOST", &config.host)
        .env("CCCTL_PORT", config.port.to_string())
        .env("OPENAI_BASE_URL", &config.responses_url)
        .env("OPENAI_API_KEY", &config.openai_key)
        .env("OPENAI_MODEL_NAME", &config.model)
        .env("CCCTL_LOG_PATH", &config.log_file)
        .env(
            "CCCTL_MIN_MAX_OUTPUT_TOKENS",
            config.min_max_output_tokens.to_string(),
        )
        .env(
            "CCCTL_FALLBACK_MAX_OUTPUT_TOKENS",
            config.fallback_max_output_tokens.to_string(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("failed to start proxy child: {}", e))
}

async fn launch_claude_mode(config: Config, claude_args: Vec<String>) -> Result<(), String> {
    let base_url = format!("http://{}:{}", config.host, config.port);
    let mut proxy_child = spawn_proxy_child()?;
    if let Err(err) = wait_for_proxy_ready(&base_url, &mut proxy_child).await {
        let _ = proxy_child.kill();
        let _ = proxy_child.wait();
        return Err(err);
    }

    let force_bare = !claude_args.iter().any(|arg| arg == "--bare");
    let mut child = Command::new("claude");
    if force_bare {
        child.arg("--bare");
    }
    child.args(claude_args);
    child.env("ANTHROPIC_BASE_URL", &base_url);
    child.env(
        "ANTHROPIC_API_KEY",
        std::env::var("ANTHROPIC_API_KEY").unwrap_or_else(|_| "ccp".to_string()),
    );
    child.env("CLAUDE_CODE_SIMPLE", "1");
    child.stdin(Stdio::inherit());
    child.stdout(Stdio::inherit());
    child.stderr(Stdio::inherit());

    let child = child
        .spawn()
        .map_err(|e| format!("failed to start claude: {}", e));
    let mut child = match child {
        Ok(child) => child,
        Err(err) => {
            let _ = proxy_child.kill();
            let _ = proxy_child.wait();
            return Err(err);
        }
    };

    let child_status = tokio::task::spawn_blocking(move || {
        child
            .wait()
            .map_err(|e| format!("failed to wait claude: {}", e))
    })
    .await
    .map_err(|e| format!("claude wait task join error: {}", e))??;

    let _ = proxy_child.kill();
    let _ = tokio::task::spawn_blocking(move || proxy_child.wait())
        .await
        .map_err(|e| format!("proxy wait task join error: {}", e));

    if !child_status.success() {
        return Err(format!("claude exited with status {}", child_status));
    }

    Ok(())
}

fn truncate(text: &str, limit: usize) -> String {
    let mut out = String::new();
    for ch in text.chars().take(limit) {
        out.push(ch);
    }
    if out.len() == text.len() {
        out
    } else {
        format!("{}...<truncated>", out)
    }
}

fn log_request_summary(
    model_alias: &str,
    messages_len: usize,
    system_prompt_present: bool,
    tools_count: usize,
    max_tokens: i64,
    stream: bool,
    reasoning_effort: Option<&str>,
    top_p_present: bool,
) {
    log::info!(
        "proxy request model={} messages={} system={} tools={} max_tokens={} stream={} reasoning={} top_p={}",
        model_alias,
        messages_len,
        system_prompt_present,
        tools_count,
        max_tokens,
        stream,
        reasoning_effort.unwrap_or("none"),
        top_p_present,
    );
}

fn log_forwarded_request(body: &Value) {
    let serialized = serde_json::to_string(body).unwrap_or_else(|_| body.to_string());
    log::info!("openai request body={}", serialized);
}

fn log_openai_response(status: StatusCode, body: &str) {
    log::info!("openai response status={} body={}", status, body);
}

fn log_openai_stream_event(data: &str) {
    log::info!("openai stream data={}", data);
}

async fn log_http_request(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    log::info!("incoming request method={} uri={}", method, uri);

    let started_at = std::time::Instant::now();
    let response = next.run(req).await;

    log::info!(
        "completed request method={} uri={} status={} elapsed_ms={}",
        method,
        uri,
        response.status(),
        started_at.elapsed().as_millis()
    );

    response
}

fn coerce_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => trimmed_owned(s),
        Value::Array(items) => collect_text_items(items),
        Value::Object(map) => map
            .get("text")
            .and_then(|v| v.as_str())
            .and_then(trimmed_owned),
        _ => trimmed_owned(&value.to_string()),
    }
}

fn trimmed_owned(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn collect_text_items(items: &[Value]) -> Option<String> {
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::String(text) => {
                if let Some(text) = trimmed_owned(text) {
                    parts.push(text);
                }
            }
            Value::Object(map) => {
                if let Some(Value::String(text)) = map.get("text") {
                    if let Some(text) = trimmed_owned(text) {
                        parts.push(text);
                    }
                }
            }
            _ => {}
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn format_tool_block(block: &Map<String, Value>) -> Option<String> {
    match block.get("type").and_then(|v| v.as_str()) {
        Some("tool_use") => {
            let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
            let id = block.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let input = block
                .get("input")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let input_text = serde_json::to_string(&input).unwrap_or_else(|_| input.to_string());
            Some(format!("[tool_use name={} id={}] {}", name, id, input_text))
        }
        Some("tool_result") => {
            let tool_use_id = block
                .get("tool_use_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let is_error = block
                .get("is_error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let content = block.get("content").cloned().unwrap_or(Value::Null);
            let content_text = match content {
                Value::Array(items) => collect_text_items(&items).unwrap_or_default(),
                other => coerce_text(&other).unwrap_or_default(),
            };
            let mut rendered = format!(
                "[tool_result tool_use_id={} is_error={}]",
                tool_use_id, is_error
            );
            if !content_text.is_empty() {
                rendered.push(' ');
                rendered.push_str(&content_text);
            }
            Some(rendered)
        }
        Some("text") => block
            .get("text")
            .and_then(|v| v.as_str())
            .and_then(trimmed_owned),
        _ => None,
    }
}

fn normalize_tools(tools: Option<&Value>) -> Option<Vec<Value>> {
    let tools = tools?.as_array()?;
    let mut normalized = Vec::with_capacity(tools.len());

    for tool in tools {
        let Some(obj) = tool.as_object() else {
            continue;
        };

        if obj.get("type").and_then(|v| v.as_str()) == Some("function") && obj.contains_key("name")
        {
            normalized.push(Value::Object(obj.clone()));
            continue;
        }

        let Some(name) = obj.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }

        let mut converted = Map::new();
        converted.insert("type".into(), Value::String("function".into()));
        converted.insert("name".into(), Value::String(name.to_string()));

        if let Some(desc) = obj.get("description").and_then(|v| v.as_str()) {
            let desc = desc.trim();
            if !desc.is_empty() {
                converted.insert("description".into(), Value::String(desc.to_string()));
            }
        }

        let parameters = obj
            .get("parameters")
            .cloned()
            .or_else(|| obj.get("input_schema").cloned())
            .unwrap_or_else(|| json!({"type":"object","properties":{}}));
        converted.insert("parameters".into(), parameters);
        normalized.push(Value::Object(converted));
    }

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn anthropic_messages_to_responses_input(messages: &[Value]) -> Vec<Value> {
    let mut input_payload = Vec::with_capacity(messages.len());
    for msg in messages {
        let Some(obj) = msg.as_object() else {
            continue;
        };

        let role = obj.get("role").and_then(|v| v.as_str()).unwrap_or("user");
        let content = obj.get("content").cloned().unwrap_or(Value::Null);

        let text = if let Value::Array(blocks) = content {
            let mut parts = Vec::with_capacity(blocks.len());
            for block in blocks {
                if let Some(block_obj) = block.as_object() {
                    if let Some(rendered) = format_tool_block(block_obj) {
                        if !rendered.is_empty() {
                            parts.push(rendered);
                        }
                    }
                }
            }
            let joined = parts.join("\n");
            if joined.trim().is_empty() {
                None
            } else {
                Some(joined)
            }
        } else {
            coerce_text(&content)
        };

        let Some(text) = text else {
            continue;
        };

        if text.is_empty() {
            continue;
        }

        let role = if role == "assistant" || role == "user" {
            role
        } else {
            "user"
        };
        input_payload.push(json!({"role": role, "content": text}));
    }
    input_payload
}

fn upstream_error_detail(status: StatusCode, body: &str, req_body: &Value) -> Value {
    json!({
        "message": "Upstream OpenAI request failed",
        "upstream_status": status.as_u16(),
        "upstream_detail": truncate(body, 2000),
        "request_model": req_body.get("model"),
        "request_stream": req_body.get("stream"),
        "request_max_output_tokens": req_body.get("max_output_tokens"),
    })
}

fn convert_responses_non_streaming(response_json: &Value, model_id: &str) -> Value {
    let mut text_content = String::new();
    if let Some(output) = response_json.get("output").and_then(|v| v.as_array()) {
        for item in output {
            if item.get("type").and_then(|v| v.as_str()) == Some("message") {
                if let Some(content_items) = item.get("content").and_then(|v| v.as_array()) {
                    for content_item in content_items {
                        if content_item.get("type").and_then(|v| v.as_str()) == Some("output_text")
                        {
                            if let Some(text) = content_item.get("text").and_then(|v| v.as_str()) {
                                text_content.push_str(text);
                            }
                        }
                    }
                }
            }
        }
    }

    let usage = response_json
        .get("usage")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let input_tokens = usage
        .get("input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    json!({
        "id": format!("msg_{}", Uuid::new_v4().simple()),
        "type": "message",
        "role": "assistant",
        "model": model_id,
        "content": [{"type": "text", "text": text_content}],
        "stop_reason": "end_turn",
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }
    })
}

#[derive(Default)]
struct StreamStateForResponses {
    started: bool,
    next_content_block_index: usize,
    blocks_by_item_id: HashMap<String, usize>,
    items_by_id: HashMap<String, Value>,
    tool_args_by_item_id: HashMap<String, String>,
    seen_tool_use: bool,
    finished: bool,
}

impl StreamStateForResponses {
    fn reserve_block_index(&mut self, item_id: &str) -> (usize, bool) {
        match self.blocks_by_item_id.entry(item_id.to_owned()) {
            Entry::Occupied(entry) => (*entry.get(), false),
            Entry::Vacant(entry) => {
                let block_index = self.next_content_block_index;
                self.next_content_block_index += 1;
                entry.insert(block_index);
                (block_index, true)
            }
        }
    }

    fn start_text_block(&mut self, item_id: &str) -> Vec<String> {
        let (block_index, inserted) = self.reserve_block_index(item_id);
        if !inserted {
            return Vec::new();
        }
        let block_start = json!({
            "type": "content_block_start",
            "index": block_index,
            "content_block": {"type": "text", "text": ""}
        });
        vec![format!("event: content_block_start\ndata: {}", block_start)]
    }

    fn stop_block(&mut self, item_id: &str) -> Vec<String> {
        let Some(index) = self.blocks_by_item_id.remove(item_id) else {
            return Vec::new();
        };
        let block_stop = json!({
            "type": "content_block_stop",
            "index": index
        });
        vec![format!("event: content_block_stop\ndata: {}", block_stop)]
    }

    fn process_responses_event(
        &mut self,
        event: &Value,
        model: &str,
        message_id: &str,
    ) -> Vec<String> {
        let mut events = Vec::new();
        let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let item_id = event.get("item_id").and_then(|v| v.as_str());

        if event_type == "response.created" && !self.started {
            self.started = true;
            let msg_start = json!({
                "type": "message_start",
                "message": {
                    "id": message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": Value::Null,
                    "stop_sequence": Value::Null,
                    "usage": {"input_tokens": 0, "output_tokens": 0}
                }
            });
            events.push(format!("event: message_start\ndata: {}", msg_start));
        } else if event_type == "response.output_item.added" {
            let item = event.get("item").cloned().unwrap_or_else(|| json!({}));
            let cache_id = item_id
                .or_else(|| item.get("id").and_then(|v| v.as_str()))
                .or_else(|| item.get("call_id").and_then(|v| v.as_str()))
                .map(|s| s.to_string());
            if let Some(cache_id) = cache_id {
                let is_function_call =
                    item.get("type").and_then(|v| v.as_str()) == Some("function_call");
                self.items_by_id.insert(cache_id.clone(), item);
                if is_function_call {
                    self.tool_args_by_item_id.entry(cache_id).or_default();
                }
            }
        } else if event_type == "response.output_text.delta" {
            let fallback_item_id;
            let item_id = if let Some(item_id) = item_id {
                item_id
            } else {
                fallback_item_id = format!("text_{}", self.next_content_block_index);
                &fallback_item_id
            };
            events.extend(self.start_text_block(item_id));
            if let Some(delta) = event.get("delta").and_then(|v| v.as_str()) {
                if !delta.is_empty() {
                    let index = self.blocks_by_item_id.get(item_id).copied().unwrap_or(0);
                    let content_delta = json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {"type": "text_delta", "text": delta}
                    });
                    events.push(format!(
                        "event: content_block_delta\ndata: {}",
                        content_delta
                    ));
                }
            }
        } else if event_type == "response.function_call_arguments.delta" {
            if let Some(item_id) = item_id {
                if let Some(delta) = event.get("delta").and_then(|v| v.as_str()) {
                    if !delta.is_empty() {
                        self.tool_args_by_item_id
                            .entry(item_id.to_string())
                            .and_modify(|s| s.push_str(delta))
                            .or_insert_with(|| delta.to_string());
                    }
                }
            }
        } else if event_type == "response.output_text.done"
            || event_type == "response.function_call_arguments.done"
        {
            if event_type == "response.function_call_arguments.done" {
                if let Some(item_id) = item_id {
                    let item = self
                        .items_by_id
                        .get(item_id)
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    let tool_name = item
                        .get("name")
                        .and_then(|v| v.as_str())
                        .and_then(trimmed_owned);
                    let tool_id = item
                        .get("call_id")
                        .and_then(|v| v.as_str())
                        .or_else(|| item.get("id").and_then(|v| v.as_str()))
                        .unwrap_or(item_id);
                    let args_text = event
                        .get("arguments")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| self.tool_args_by_item_id.get(item_id).cloned())
                        .unwrap_or_default();

                    if let Some(tool_name) = tool_name {
                        let block_index = self.reserve_block_index(item_id).0;

                        let block_start = json!({
                            "type": "content_block_start",
                            "index": block_index,
                            "content_block": {
                                "type": "tool_use",
                                "id": tool_id,
                                "name": tool_name,
                            }
                        });
                        events.push(format!("event: content_block_start\ndata: {}", block_start));
                        if !args_text.is_empty() {
                            let block_delta = json!({
                                "type": "content_block_delta",
                                "index": block_index,
                                "delta": {
                                    "type": "input_json_delta",
                                    "partial_json": args_text,
                                }
                            });
                            events
                                .push(format!("event: content_block_delta\ndata: {}", block_delta));
                        }
                        self.seen_tool_use = true;
                        events.extend(self.stop_block(item_id));
                    }
                    self.tool_args_by_item_id.remove(item_id);
                }
            } else if let Some(item_id) = item_id {
                events.extend(self.stop_block(item_id));
            }
        } else if event_type == "response.output_item.done" {
            let item = event.get("item").cloned().unwrap_or_else(|| json!({}));
            if item.get("type").and_then(|v| v.as_str()) == Some("message") {
                if let Some(item_id) = item_id {
                    events.extend(self.stop_block(item_id));
                }
            }
        } else if event_type == "response.completed" && !self.finished {
            self.finished = true;
            let dangling_ids: Vec<String> = self.blocks_by_item_id.keys().cloned().collect();
            for dangling_item_id in dangling_ids {
                events.extend(self.stop_block(&dangling_item_id));
            }
            let output_tokens = event
                .get("response")
                .and_then(|r| r.get("usage"))
                .and_then(|u| u.get("output_tokens"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let stop_reason = if self.seen_tool_use {
                "tool_use"
            } else {
                "end_turn"
            };
            let msg_delta = json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": Value::Null},
                "usage": {"output_tokens": output_tokens},
            });
            events.push(format!("event: message_delta\ndata: {}", msg_delta));
            events.push("event: message_stop\ndata: {\"type\":\"message_stop\"}".to_string());
        }

        events
    }
}

async fn proxy_messages(State(state): State<AppState>, body: String) -> impl IntoResponse {
    let body_value: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            let detail = json!({
                "message": "Invalid JSON",
                "body_preview": truncate(&body, 2000),
            });
            log::error!("Invalid JSON received: {}", truncate(&body, 500));
            return (StatusCode::BAD_REQUEST, Json(json!({"detail": detail}))).into_response();
        }
    };

    let model_alias = body_value
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("claude")
        .to_string();
    let messages: &[Value] = body_value
        .get("messages")
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    let system_prompt = body_value
        .get("system")
        .and_then(coerce_text)
        .or_else(|| body_value.get("instructions").and_then(coerce_text));
    let mut max_tokens = body_value
        .get("max_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(4096);
    let stream = body_value
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let tools = normalize_tools(body_value.get("tools"));
    let tool_choice = body_value.get("tool_choice").cloned();
    let parallel_tool_calls = body_value.get("parallel_tool_calls").cloned();
    let reasoning_effort = body_value
        .get("output_config")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("effort"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if max_tokens < state.config.min_max_output_tokens {
        max_tokens = state.config.fallback_max_output_tokens;
    }

    let tools_count = tools.as_ref().map(|v| v.len()).unwrap_or(0);
    log_request_summary(
        &model_alias,
        messages.len(),
        system_prompt.is_some(),
        tools_count,
        max_tokens,
        stream,
        reasoning_effort.as_deref(),
        body_value.get("top_p").is_some(),
    );

    let input_payload = anthropic_messages_to_responses_input(&messages);
    let mut responses_body = Map::new();
    responses_body.insert("input".into(), Value::Array(input_payload));
    responses_body.insert("model".into(), Value::String(state.config.model.clone()));
    responses_body.insert("max_output_tokens".into(), Value::from(max_tokens));
    responses_body.insert("stream".into(), Value::Bool(stream));
    if let Some(sys) = &system_prompt {
        responses_body.insert("instructions".into(), Value::String(sys.clone()));
    }
    if let Some(tools) = tools {
        responses_body.insert("tools".into(), Value::Array(tools));
    }
    if let Some(tool_choice) = tool_choice {
        responses_body.insert("tool_choice".into(), tool_choice);
    }
    if let Some(parallel_tool_calls) = parallel_tool_calls {
        responses_body.insert("parallel_tool_calls".into(), parallel_tool_calls);
    }
    if let Some(effort) = reasoning_effort {
        responses_body.insert("reasoning".into(), json!({"effort": effort}));
    }
    if let Some(top_p) = body_value.get("top_p") {
        responses_body.insert("top_p".into(), top_p.clone());
    }

    let responses_body_value = Value::Object(responses_body);
    log_forwarded_request(&responses_body_value);
    if stream {
        let message_id = format!("msg_{}", Uuid::new_v4().simple());
        let config = state.config.clone();
        let client = state.client.clone();
        let model_alias_clone = model_alias.clone();
        let responses_body_clone = responses_body_value.clone();
        let stream_body = stream! {
            let resp = match client
                .post(&config.responses_url)
                .json(&responses_body_clone)
                .header("api-key", &config.openai_key)
                .header("Content-Type", "application/json")
                .timeout(Duration::from_secs(300))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    log::error!("Upstream stream request failed: {}", err);
                    yield Bytes::from(format!(
                        "event: error\ndata: {}\n\n",
                        json!({
                            "type": "error",
                            "error": {
                                "type": "api_error",
                                "message": truncate(&format!("OpenAI request failed: {}", err), 2000),
                            }
                        })
                    ));
                    return;
                }
            };

            let status = resp.status();
            if status != StatusCode::OK {
                let text = resp.text().await.unwrap_or_default();
                log_openai_response(status, &text);
                log::error!("Upstream stream response status={} body={}", status, truncate(&text, 2000));
                yield Bytes::from(format!(
                    "event: error\ndata: {}\n\n",
                    json!({
                        "type": "error",
                        "error": {
                            "type": "api_error",
                            "message": truncate(&format!("OpenAI error {}: {}", status, text), 2000),
                        }
                    })
                ));
                return;
            }

            log::info!("openai response status={} streaming=true", status);

            let mut stream_state = StreamStateForResponses::default();
            let mut buf = String::new();
            let mut bytes = resp.bytes_stream();
            let mut done = false;
            while !done {
                let Some(chunk) = bytes.next().await else {
                    break;
                };
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("Upstream stream chunk error: {}", e);
                        break;
                    }
                };
                buf.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].trim_end_matches('\r').to_owned();
                    buf.drain(..=pos);
                    if line.is_empty() || !line.starts_with("data:") {
                        continue;
                    }
                    let data_str = line[5..].trim().to_string();
                    if data_str == "[DONE]" {
                        done = true;
                        break;
                    }
                    log_openai_stream_event(&data_str);
                    let Ok(event) = serde_json::from_str::<Value>(&data_str) else {
                        continue;
                    };
                    let anthropic_events =
                        stream_state.process_responses_event(&event, &model_alias_clone, &message_id);
                    for ae in anthropic_events {
                        yield Bytes::from(format!("{}\n\n", ae));
                    }
                }
            }
        };

        let mut response =
            Response::new(Body::from_stream(stream_body.map(Ok::<Bytes, Infallible>)));
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        response.headers_mut().insert(
            axum::http::header::CONNECTION,
            HeaderValue::from_static("keep-alive"),
        );
        return response;
    }

    let resp = match state
        .client
        .post(&state.config.responses_url)
        .json(&responses_body_value)
        .header("api-key", &state.config.openai_key)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(300))
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            log::error!("Upstream request failed: {}", err);
            let detail = json!({
                "message": "Upstream OpenAI request failed",
                "upstream_status": 500,
                "upstream_detail": truncate(&err.to_string(), 2000),
                "request_model": state.config.model.clone(),
                "request_stream": stream,
                "request_max_output_tokens": max_tokens,
            });
            return (StatusCode::BAD_GATEWAY, Json(json!({"detail": detail}))).into_response();
        }
    };

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    log_openai_response(status, &text);
    if status != StatusCode::OK {
        let detail = upstream_error_detail(status, &text, &responses_body_value);
        log::error!("Upstream error: {}", detail);
        return (status, Json(json!({"detail": detail}))).into_response();
    }

    let response_json: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
    let anthropic_resp = convert_responses_non_streaming(&response_json, &model_alias);
    Json(anthropic_resp).into_response()
}

async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "data": [
            {
                "id": state.config.model.clone(),
                "object": "model",
                "created": 1234567890u64,
                "owned_by": "openai"
            }
        ],
        "object": "list"
    }))
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn root_get() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn root_head() -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("0"),
    );
    headers.insert(axum::http::header::ALLOW, HeaderValue::from_static("POST"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, Request};
    use log::{Level, Record};
    use tower::ServiceExt;

    fn test_state() -> AppState {
        AppState {
            client: Client::builder()
                .build()
                .expect("failed to build reqwest client"),
            config: Config {
                responses_url: "http://localhost".to_string(),
                openai_key: "test-key".to_string(),
                model: "test-model".to_string(),
                host: "127.0.0.1".to_string(),
                port: 5520,
                log_file: "test.log".to_string(),
                log_level: LevelFilter::Info,
                min_max_output_tokens: 8192,
                fallback_max_output_tokens: 8192,
            },
        }
    }

    #[test]
    fn logger_does_not_create_file_until_first_log() {
        let log_file = format!(
            "{}/ccctl-test-{}.log",
            std::env::temp_dir().display(),
            Uuid::new_v4().simple()
        );
        let _ = std::fs::remove_file(&log_file);

        let logger = MultiLogger {
            file: Mutex::new(None),
            log_file: log_file.clone(),
            max_level: LevelFilter::Error,
        };

        assert!(!std::path::Path::new(&log_file).exists());

        let record = Record::builder()
            .args(format_args!("test log"))
            .level(Level::Error)
            .target("tests")
            .build();
        logger.log(&record);

        assert!(std::path::Path::new(&log_file).exists());

        let _ = std::fs::remove_file(&log_file);
    }

    #[tokio::test]
    async fn head_root_returns_method_not_allowed() {
        let app = build_app(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_LENGTH),
            Some(&HeaderValue::from_static("0"))
        );
        assert_eq!(
            response.headers().get(axum::http::header::ALLOW),
            Some(&HeaderValue::from_static("POST"))
        );
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
            Bytes::new()
        );
    }


    #[tokio::test]
    async fn get_root_returns_json() {
        let app = build_app(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(body, Bytes::from_static(br#"{"status":"ok"}"#));
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let first_arg = args.next();
    let config = build_config();

    if config.openai_key.is_empty() {
        panic!("Please set OPENAI_API_KEY in the environment");
    }
    init_logger(&config.log_file, config.log_level);

    match first_arg.as_deref() {
        Some("claude") => {
            let claude_args: Vec<String> = args.collect();
            if let Err(err) = launch_claude_mode(config, claude_args).await {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        }
        Some("proxy") => {
            if let Err(err) = run_proxy_server(config).await {
                eprintln!("proxy server error: {}", err);
                std::process::exit(1);
            }
        }
        Some(other) => {
            eprintln!(
                "Unknown mode '{}'. Use no arguments for proxy mode, 'proxy' for proxy-only mode, or 'claude' for launcher mode.",
                other
            );
            std::process::exit(2);
        }
        None => {
            if let Err(err) = run_proxy_server(config).await {
                eprintln!("proxy server error: {}", err);
                std::process::exit(1);
            }
        }
    }
}
