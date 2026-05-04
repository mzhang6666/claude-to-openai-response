# Claude Code OpenAI Proxy

Use `ccctl claude` to connect Claude to an OpenAI Responses API backend.

`ccctl` is a small Rust binary that lets Claude Code client talk to an OpenAI Responses API backend.

The project is designed to be built once and run as a compiled executable named `ccctl`.

## What It Does

`ccctl` acts as a protocol bridge:

- Incoming requests use an Anthropic / Claude-compatible message format.
- The proxy translates those requests into OpenAI Responses API calls.
- The upstream response is translated back into Claude-compatible JSON or SSE output.

This makes it possible to use OpenAI models with tools that expect Claude-style endpoints.

## Main Modes

- `ccctl proxy`: start the local HTTP proxy only.
- `ccctl claude`: start the proxy, wait until it is ready, then launch the `claude` CLI through it.

If no subcommand is provided, the binary also starts in proxy mode.

## Architecture

```text
┌──────────────────────────┐
│ Claude Code / client      │
│ Sends Anthropic-style API │
└─────────────┬────────────┘
              │
              │ POST /v1/messages
              ▼
┌──────────────────────────┐
│ ccctl                    │
│ - normalizes messages    │
│ - maps tools             │
│ - maps stream events     │
│ - injects model metadata │
└─────────────┬────────────┘
              │
              │ OpenAI Responses API
              ▼
┌──────────────────────────┐
│ OpenAI-compatible backend │
│ Model + usage + SSE       │
└─────────────┬────────────┘
              │
              │ translated response
              ▼
┌──────────────────────────┐
│ Claude-compatible output  │
│ JSON or SSE stream        │
└──────────────────────────┘
```

## Build

Build the release binary with Cargo:

```bash
cargo build --release
```

The compiled executable will be available at:

```bash
./target/release/ccctl
```

## Requirements

- Rust toolchain
- An OpenAI-compatible backend that supports the Responses API
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL_NAME`
- Optional: `claude` CLI, only if you want launcher mode

## Configuration

`ccctl` reads its configuration from environment variables.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `OPENAI_BASE_URL` | Yes | `""` | Base URL for the upstream Responses API |
| `OPENAI_API_KEY` | Yes | `""` | API key used for upstream requests |
| `OPENAI_MODEL_NAME` | Yes | `""` | Model name used when forwarding requests |
| `CCCTL_HOST` | No | `127.0.0.1` | Bind host for the local proxy |
| `CCCTL_PORT` | No | `5520` | Bind port for the local proxy |
| `CCCTL_LOG_PATH` | No | `ccctl.log` | Log file path |
| `CCCTL_LOG_LEVEL` | No | `info` | Log level, for example `error`, `warn`, `info`, `debug`, or `trace` |
| `CCCTL_MIN_MAX_OUTPUT_TOKENS` | No | `8192` | Threshold for fallback token handling |
| `CCCTL_FALLBACK_MAX_OUTPUT_TOKENS` | No | `8192` | Fallback max output tokens value |
| `ANTHROPIC_API_KEY` | No | `ccp` | Used only in `claude` launcher mode |

## Usage

### 1) Run the proxy only

Set the upstream variables and start the binary:

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

### 2) Launch Claude through the proxy

In launcher mode, `ccctl` starts the proxy first, waits for `/v1/health`, then launches `claude` with the local proxy URL.

```bash
export OPENAI_BASE_URL="https://your-upstream.example.com/v1/responses"
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL_NAME="your-model"

./target/release/ccctl claude
```

Any extra arguments after `claude` are forwarded to the `claude` command.

### 3) Health check

```bash
curl http://127.0.0.1:5520/v1/health
```

Expected response:

```json
{"status":"ok"}
```

### 4) Model listing

```bash
curl http://127.0.0.1:5520/v1/models
```

The server returns the configured upstream model as a single-item model list.

## Tool Usage

The proxy accepts Claude-style tool definitions in `POST /v1/messages` and normalizes them before forwarding to OpenAI.

### Supported input shapes

- Function tools with `type: "function"` and `name`
- Legacy-style tool objects containing:
  - `name`
  - `description`
  - `parameters` or `input_schema`

### What the proxy does

- Passes normalized tools to the upstream Responses API.
- Preserves `tool_choice` when present.
- Preserves `parallel_tool_calls` when present.
- Converts streaming function-call deltas into Claude-style `tool_use` events.
- Converts tool results and text blocks into Claude-compatible message content.

### Example request

```bash
curl http://127.0.0.1:5520/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude",
    "messages": [
      {
        "role": "user",
        "content": "Write a short summary of Rust."
      }
    ],
    "tools": [
      {
        "name": "get_time",
        "description": "Return the current time",
        "parameters": {
          "type": "object",
          "properties": {}
        }
      }
    ],
    "stream": false
  }'
```

### Streaming

If `stream: true`, the proxy returns `text/event-stream` and maps OpenAI Responses events into Claude-compatible SSE events such as:

- `message_start`
- `content_block_start`
- `content_block_delta`
- `content_block_stop`
- `message_delta`
- `message_stop`

## Endpoints

### `POST /v1/messages`

Main proxy endpoint. It:

- accepts Anthropic-style message payloads
- forwards the request to the upstream Responses API
- returns Claude-compatible JSON or SSE

### `GET /v1/models`

Returns a minimal model list with the configured upstream model.

### `GET /v1/health`

Returns `{"status":"ok"}` for readiness checks.

## Logging and Errors

- Logs are written to the file configured by `CCCTL_LOG_PATH`.
- `CCCTL_LOG_LEVEL` controls whether the request/response `info` logs are emitted.
- The process exits immediately if `OPENAI_API_KEY` is missing.
- Upstream failures return detailed error payloads to help with debugging.

## Project Layout

```text
src/
  main.rs      # server, proxy logic, stream conversion, launcher mode
Cargo.toml     # dependencies and crate metadata
Cargo.lock     # locked dependency versions
README.md      # usage guide and project overview
```
