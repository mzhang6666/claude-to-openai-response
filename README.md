# Claude Code OpenAI Proxy

`ccctl` is a small Rust binary that proxies Claude Code / Anthropic-style requests to an OpenAI Responses API backend, then translates the upstream response back into Claude-compatible JSON or SSE.

It supports two modes:

- `ccctl proxy` — start the local HTTP proxy only
- `ccctl claude` — start the proxy, wait until it is ready, then launch the `claude` CLI through it

## Architecture

```text
┌──────────────────────────┐
│ Claude Code / Claude CLI │
│ Sends Anthropic-style API │
└─────────────┬────────────┘
              │ POST /v1/messages
              ▼
┌──────────────────────────┐
│ ccctl                    │
│ - loads environment cfg  │
│ - normalizes messages    │
│ - normalizes tools       │
│ - forwards to OpenAI     │
│ - maps responses back    │
└─────────────┬────────────┘
              │ OpenAI Responses API
              ▼
┌──────────────────────────┐
│ Upstream backend          │
│ Returns JSON or SSE        │
└─────────────┬────────────┘
              │ translated output
              ▼
┌──────────────────────────┐
│ Claude-compatible output  │
│ JSON or text/event-stream │
└──────────────────────────┘
```

## Request Flow

1. A client sends a Claude / Anthropic-style request to `POST /v1/messages`.
2. `ccctl` reads `model`, `messages`, `system`, `tools`, `tool_choice`, `parallel_tool_calls`, `stream`, and related fields.
3. Messages are converted into the OpenAI Responses API input format.
4. Tool definitions are normalized into OpenAI function tool objects.
5. `ccctl` forwards the request to the upstream Responses API.
6. Non-streaming responses are converted into Claude-compatible JSON.
7. Streaming responses are converted into Claude-compatible SSE events.
8. In `ccctl claude` mode, the proxy is started first, health-checked, and then the `claude` CLI is launched.

## Key Functions

### Configuration and startup

- `build_config()` — reads environment variables and builds runtime configuration.
- `init_logger()` — installs the logger and writes logs to both stderr and the log file.
- `run_proxy_server()` — starts the local HTTP server.
- `launch_claude_mode()` — starts the proxy and launches the `claude` CLI.
- `main()` — parses command-line arguments and selects proxy mode or launcher mode.

### Request translation

- `proxy_messages()` — main handler for `POST /v1/messages`.
- `anthropic_messages_to_responses_input()` — converts Claude messages into Responses API input.
- `normalize_tools()` — converts tool definitions into a normalized function-tool shape.
- `coerce_text()` / `collect_text_items()` — extract text from request payloads.
- `upstream_error_detail()` — builds detailed upstream error payloads.

### Response translation

- `convert_responses_non_streaming()` — converts non-streaming Responses output into Claude JSON.
- `StreamStateForResponses` — tracks state while mapping streaming events.
- `process_responses_event()` — maps OpenAI streaming events to Claude SSE events.

### Routing and helpers

- `build_app()` — assembles the Axum router.
- `health()` — readiness endpoint.
- `list_models()` — returns a minimal model list.
- `root_get()` / `root_head()` — root path handlers.

## Endpoints

### `POST /v1/messages`

Main proxy endpoint.

- accepts Claude-style message payloads
- forwards the request to the upstream Responses API
- returns Claude-compatible JSON or SSE

### `GET /v1/models`

Returns a minimal model list containing the configured upstream model.

### `GET /v1/health`

Returns `{"status":"ok"}` for readiness checks.

## Streaming Output

When `stream: true`, `ccctl` returns `text/event-stream` and maps OpenAI Responses events into Claude-compatible SSE events such as:

- `message_start`
- `content_block_start`
- `content_block_delta`
- `content_block_stop`
- `message_delta`
- `message_stop`

## Tool Support

`ccctl` accepts Claude-style tool definitions in `POST /v1/messages` and normalizes them before forwarding to OpenAI.

Supported input shapes include:

- standard function tools with `type: "function"` and `name`
- legacy tool objects with:
  - `name`
  - `description`
  - `parameters` or `input_schema`

The proxy:

- converts tools into the shape expected by the Responses API
- preserves `tool_choice`
- preserves `parallel_tool_calls`
- converts streaming function-call deltas into Claude-style `tool_use` events
- converts tool results and text blocks into Claude-compatible content

## Configuration

`ccctl` reads configuration from environment variables.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `OPENAI_BASE_URL` | Yes | `""` | Upstream Responses API base URL |
| `OPENAI_API_KEY` | Yes | `""` | API key used for upstream requests |
| `OPENAI_MODEL_NAME` | Yes | `""` | Model name used when forwarding requests |
| `CCCTL_HOST` | No | `127.0.0.1` | Local proxy bind host |
| `CCCTL_PORT` | No | `5520` | Local proxy bind port |
| `CCCTL_LOG_PATH` | No | `ccctl.log` | Log file path |
| `CCCTL_LOG_LEVEL` | No | `off` | Log level, for example `error`, `warn`, `info`, or `debug` |
| `CCCTL_MIN_MAX_OUTPUT_TOKENS` | No | `8192` | Threshold for fallback token handling |
| `CCCTL_FALLBACK_MAX_OUTPUT_TOKENS` | No | `8192` | Fallback max output tokens value |
| `ANTHROPIC_API_KEY` | No | `ccp` | Used only in `ccctl claude` mode |

## Build

```bash
cargo build --release
```

The compiled binary is available at:

```bash
./target/release/ccctl
```

## Usage

### 1. Run the proxy only

```bash
export OPENAI_BASE_URL="https://your-upstream.example.com/v1/responses"
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL_NAME="your-model"

./target/release/ccctl proxy
```

You can also omit the subcommand and start the proxy directly:

```bash
./target/release/ccctl
```

### 2. Launch Claude through the proxy

```bash
export OPENAI_BASE_URL="https://your-upstream.example.com/v1/responses"
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL_NAME="your-model"

./target/release/ccctl claude
```

Any extra arguments after `claude` are forwarded to the `claude` command.

## Logging and Errors

- Logs are written to the file configured by `CCCTL_LOG_PATH`.
- `CCCTL_LOG_LEVEL` controls whether request and response `info` logs are emitted.
- The process exits immediately if `OPENAI_API_KEY` is missing.
- Upstream failures return detailed error payloads to help with debugging.

## Project Layout

```text
src/
  main.rs      # server entrypoint, proxy logic, stream conversion, launcher mode
Cargo.toml     # crate metadata and dependencies
Cargo.lock     # locked dependency versions
README.md      # project overview and usage guide
```
