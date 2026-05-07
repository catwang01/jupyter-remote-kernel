# CLAUDE.md — jupyter-remote-kernel

## Project Overview

A reverse WebSocket tunnel that exposes Jupyter kernels from remote machines (behind NAT/firewalls) to a local JupyterLab via the Jupyter Gateway API.

**Two deployment modes**:
- **Extension mode** (recommended): Hub embedded in JupyterLab as a Jupyter Server Extension, shares auth
- **Standalone mode**: Hub runs as a separate process (suitable for public servers)

## File Structure

```
src/jupyter_remote_kernel/
├── __init__.py          # MUST expose _jupyter_server_extension_points (see below)
├── hub.py               # Standalone Hub (aiohttp)
├── server_extension.py  # Extension Hub (Tornado, embedded in JupyterLab)
├── agent.py             # Remote agent (runs on each remote machine)
└── cli.py               # CLI: `hub` and `agent` subcommands
jupyter-config/
└── server_config.d/
    └── jupyter_remote_kernel.json  # Auto-enables extension on install
pyproject.toml           # Package metadata + extension entry points
```

## Architecture

```
JupyterLab (local)
  └── GatewayClient
        │  HTTP/WS
        ▼
      Hub                            ← Jupyter Gateway API compatible
      (embedded in JupyterLab or standalone process)
        │  WebSocket tunnel (single connection, multiplexed)
        ▼
      RemoteAgent (agent.py)         ← remote machine behind NAT
        │  local HTTP/WS
        ▼
      Jupyter Server (remote local)
```

### Extension mode

- Hub endpoints mounted under `/jrk/` (e.g. `/jrk/api/kernels`)
- JupyterLab's `@authenticated` decorator protects all HTTP endpoints automatically
- `KernelChannelsHandler` (WS) uses standard `@authenticated` — JRK's patched `start_channels()` passes the auth token explicitly in `create_connection()` headers
- `GatewayClient.url=http://localhost:<port>/jrk` points to itself (not a loop: browser → JupyterLab server → extension handler → agent)
- Agent authenticates with `Authorization: token <jupyterlab-token>` header

### Standalone mode

- Hub listens on its own port, supports `--token` option
- Agent authenticates via `token` field in the register message
- API endpoints authenticated via `Authorization: token <token>` header (aiohttp middleware)

## Gateway API Compliance

All endpoints called by GatewayClient and their implementation status:

### KernelSpec endpoints

| Endpoint | Status |
|----------|--------|
| `GET /api/kernelspecs` | ✅ implemented |
| `GET /api/kernelspecs/{kernel_name}` | ✅ implemented |
| `GET /kernelspecs/{kernel_name}/{resource}` | ✅ implemented |

### Kernel endpoints

| Endpoint | Status |
|----------|--------|
| `GET /api/kernels` | ✅ implemented |
| `POST /api/kernels` | ✅ implemented |
| `GET /api/kernels/{kernel_id}` | ✅ implemented |
| `DELETE /api/kernels/{kernel_id}` | ✅ implemented |
| `POST /api/kernels/{kernel_id}/restart` | ✅ implemented |
| `POST /api/kernels/{kernel_id}/interrupt` | ✅ implemented |
| `WS /api/kernels/{kernel_id}/channels` | ✅ implemented |

## Key Design Decisions

1. **No ZMQ bridging** — The remote Jupyter Server handles ZMQ internally. We only proxy at the HTTP/WS level (single port).
2. **Single tunnel WebSocket** — All traffic (multiple HTTP requests, multiple kernel WS connections) is multiplexed over one persistent WebSocket using `req_id` and `ws_id`.
3. **Kernel naming**: `{agent_name}:{kernel_spec_name}` (e.g., `gpu-machine:python3`). Hub splits on first `:` to route to correct agent. **Both the dict key AND the `v["name"]` field must be set** — JupyterLab uses `name` field when creating kernels.
4. **Files stay local** — JupyterLab manages files locally. Only kernel execution happens remotely.
5. **Auth sharing** — Extension mode reuses JupyterLab's token; standalone mode uses its own `--token` flag.
6. **Auto port selection** — Agent finds a free port automatically (`socket.bind(0)`), no manual `--jupyter-port` needed.

## Important Implementation Notes

- `api_kernelspecs` must return `"default": ""` (empty string, not null) — JupyterLab's GatewayMappingKernelManager expects unicode.
- **Kernelspec `name` field bug**: Hub must set `v["name"] = key` (e.g., `machine-1:python3`) in the kernelspec response. Without this, JupyterLab sends just `"python3"` when creating kernels, and Hub routes all to the first tunnel.
- **Tornado `finish()` rejects lists**: `KernelsHandler.get` aggregates results from all agents into a list. Tornado's `finish()` refuses to serialize lists directly (CSRF protection). Must use `self.finish(json.dumps(results))` with explicit `Content-Type: application/json` header.
- Agent generates a random token for its local Jupyter Server. On restart, it kills any existing process on the port to avoid token mismatch.
- Agent passes `cwd=root_dir` to subprocess — `--ServerApp.root_dir` only affects the file browser, not the kernel's cwd.
- Agent sends both `Authorization` header (for extension mode) AND `token` field in register message (for standalone mode).
- **Reverse proxy path**: When JupyterLab runs with `--ServerApp.base_url=/jupyter`, the Hub endpoint becomes `/jupyter/jrk/`. Agents must use the full path (e.g., `--hub http://host/jupyter/jrk`), not just `/jrk`.
- **Tunnel heartbeat**: Agent uses `heartbeat=30.0` on the tunnel WS (`aiohttp` ping/pong) to prevent idle connection resets. Without this, the server may close the tunnel due to inactivity, causing `Connection reset by peer` (errno 54 on macOS).
- **Kernel restore on reconnect**: When an agent disconnects, `on_close()` removes all its kernel mappings from `kernel_tunnel`. On re-registration, `_restore_kernels()` queries `GET /api/kernels` on the agent's local Jupyter Server and re-populates the mappings. Without this, existing kernels become unreachable after agent reconnect, causing "Lost connection to Gateway" loops.
- **Kernel channels WS auth**: `GatewayKernelClient.start_channels()` (from `jupyter_server`) does not pass auth headers — upstream bug. JRK's patched `_start_channels_nonblocking` fixes this by passing `{auth_header_key: auth_scheme + auth_token}` explicitly in `create_connection()`. `KernelChannelsHandler` uses standard `@authenticated`.
- **Browser WS goes through JupyterLab proxy, not directly to jrk**: Browser connects to `ws://host/api/kernels/{id}/channels` (without `/jrk/`). JupyterLab's server-side gateway proxy then connects to `ws://localhost/jrk/api/kernels/{id}/channels` internally. Browser never directly accesses `/jrk/` endpoints. When accessing behind a reverse proxy (Cloudflare/nginx/frp), WS failures are caused by JupyterLab's own auth mechanism, not JRK's.
- **Notebook kernelspec metadata**: Notebooks must have both `name` and `display_name` in `metadata.kernelspec`. Missing `display_name` causes repeated `Notebook JSON is invalid` errors on save (though it does not block execution directly).
- **Extension loading: `__init__.py` is critical**: `jupyter_server` imports the top-level package (`jupyter_remote_kernel`), NOT `server_extension.py` directly. So `__init__.py` MUST re-export `_jupyter_server_extension_points` from `server_extension.py`. Without this, extension loading fails with `_load_jupyter_server_extension function was not found`.
- **Agent starts Jupyter via `python3 -m jupyter_server`**: NOT `python3 -m jupyter server`. The latter relies on `jupyter` dispatching to a `jupyter-server` script in PATH, which fails on systems where user Python bin is not in PATH (e.g., macOS Xcode Python 3.9 with user packages at `~/Library/Python/3.9/bin`).
- **Auto-enable doesn't work with editable install**: `pip install -e .` does NOT install `data_files`. Must manually copy `jupyter-config/server_config.d/jupyter_remote_kernel.json` to `<prefix>/etc/jupyter/jupyter_server_config.d/`, or run `jupyter server extension enable jupyter_remote_kernel`.
- **Multiple Python installs on remote machines**: When deploying the agent, the `jupyter-remote-kernel` binary's shebang determines which Python runs it. If a machine has multiple Pythons (e.g., system Python 3.9, Homebrew Python 3.13, Conda), you must `pip install` using the same Python that is in PATH. Verify with `head -1 $(which jupyter-remote-kernel)`.
- **Agent `--debug` flag**: Prints all tunnel WebSocket messages (`[DEBUG] RECV/SEND`) with type, req_id/ws_id, and truncated data/body (200 chars). Useful for verifying messages flow bidirectionally.
- **GatewayKernelClient monkey-patches** (in `server_extension.py`): Two patches applied at module load time to fix compatibility with `jupyter-server-nbmodel`'s `POST /api/kernels/{id}/execute` endpoint:
  1. **`start_channels()` deadlock + auth fix**: Original `start_channels()` calls `websocket.create_connection()` synchronously (deadlock) and without auth headers (403). Fix: run in `loop.run_in_executor()` and pass `{auth_header_key: auth_scheme + auth_token}` in `header=`.
  2. **`execute_interactive()` ZMQ Poller fix**: Base class `_async_execute_interactive()` uses `zmq.asyncio.Poller` which requires a `.socket` attribute on each channel. `GatewayKernelClient` channels are `ChannelQueue` objects (WebSocket-backed) with no `.socket`. Fix: replace the ZMQ poll loop with `ChannelQueue.get_msg()` calls. Must also explicitly set `GatewayKernelClient.execute_interactive = <new_func>` because `AsyncKernelClient` assigns `execute_interactive` as a direct class attribute (bypassing MRO).

## Build & Run

```bash
pip install -e .

# Extension mode (recommended)
# MCP_TOKEN enables jupyter-mcp-server endpoint at /mcp
MCP_TOKEN=test jupyter lab --port=8890 --ServerApp.token=test \
  --GatewayClient.url=http://localhost:8890/jrk \
  --GatewayClient.auth_token=test

# Agent (connects to extension mode)
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name test --token test

# Agent with debug logging (prints all tunnel messages)
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name test --token test --debug

# Standalone mode
jupyter-remote-kernel hub --port 8765 --token my-secret
jupyter-remote-kernel agent --hub http://hub-host:8765 --name gpu-machine --token my-secret

# JupyterLab connects to standalone hub
jupyter lab --GatewayClient.url=http://hub-host:8765 --GatewayClient.auth_token=my-secret

# With base_url (e.g., behind reverse proxy)
# Agent must include the full base_url path:
jupyter-remote-kernel agent --hub http://host/jupyter/jrk --name test --token <token>
```

## Dependencies

- `aiohttp>=3.9` — async HTTP server/client (standalone Hub + agent's WS client)
- `jupyter_server>=2.0` — remote Jupyter Server (started by agent) + extension framework (Tornado handlers)
- `ipykernel` — must be installed on remote machines

### Optional (for Jupyter MCP integration)

- `jupyter-mcp-server` — MCP server endpoint at `/mcp` (SSE-based), enables AI clients to control notebooks
- `jupyter-mcp-tools` — JupyterLab frontend extension for MCP tools (echo WS at `/jupyter-mcp-tools/echo`)
- `jupyter-collaboration>=4.0.2` + `pycrdt` — real-time CRDT sync, required for MCP edits to appear live in browser

**⚠️ GatewayClient incompatibility**: `jupyter-mcp-tools` (which depends on `jupyter-server-nbmodel`) is NOT compatible with GatewayClient/JRK. Must disable both extensions when using JRK:
```bash
jupyter labextension disable @datalayer/jupyter-server-nbmodel
jupyter labextension disable @datalayer/jupyter-mcp-tools
jupyter labextension disable @jupyter/collaboration-extension
```
See "Known Issues" for details.

## Known Issues / TODO

- **Browser 302 on kernel channels WS**: When JupyterLab is accessed behind a reverse proxy (Cloudflare/nginx/frp), JupyterLab's own `@authenticated` on its native `/api/kernels/{id}/channels` endpoint may fail for external requests, returning 302. This is a JupyterLab issue, not JRK — browser never connects to `/jrk/` directly.
- **Stale Jupyter Server processes**: Agent uses `_free_port()` on each start, accumulating orphan Jupyter Server processes from previous runs (visible in `ps aux`). Consider tracking PIDs or using a fixed port with proper cleanup.
- **JupyterLab Collaboration**: `@jupyter/collaboration-extension` opens multiple simultaneous WS sessions per kernel (3 session_ids observed). Must be disabled when using GatewayClient/JRK.
- **`jupyter-server-nbmodel` incompatible with GatewayClient/JRK**: `@datalayer/jupyter-server-nbmodel` (dependency of `jupyter-mcp-tools`) breaks kernel WS connections when GatewayClient is enabled. Root causes:
  1. **Frontend**: Its `package.json` declares `"disabledExtensions": ["@jupyterlab/notebook-extension:cell-executor"]`, killing the default cell executor. Without it, the frontend cannot establish kernel WS channels.
  2. **Backend (deadlock)**: `execution_stack.py` calls `GatewayKernelManager.client()` → returns `GatewayKernelClient` → `kernel_worker` in `actions.py` calls `GatewayKernelClient.start_channels()` → calls `websocket.create_connection()` synchronously, blocking the event loop. Since Hub runs in the same Tornado process, the event loop can't process the WS upgrade → deadlock → `WebSocketTimeoutException`. Additionally, `start_channels()` does not pass auth headers (upstream `jupyter_server` bug).
  3. **Backend (ZMQ incompatibility)**: After `start_channels()` succeeds, `execute_interactive()` uses `zmq.asyncio.Poller` requiring a `.socket` attribute, but `GatewayKernelClient` channels are `ChannelQueue` objects with no `.socket` → `AttributeError`.
  4. Registers a non-standard `POST /api/kernels/{id}/execute` HTTP endpoint (visible in browser network tab as `/execute?<timestamp>`).
  - **JRK patches applied** (in `server_extension.py`): Monkey-patches `GatewayKernelClient.start_channels` (non-blocking via `run_in_executor`) and `execute_interactive` (uses `ChannelQueue.get_msg()` instead of ZMQ Poller). Verified working on local Mac (`POST /execute` returns `{"status": "ok"}`).
  - **Frontend fix still needed**: Must `jupyter labextension disable @datalayer/jupyter-server-nbmodel && jupyter labextension disable @datalayer/jupyter-mcp-tools` — the disabled cell executor (issue 1) cannot be fixed from JRK.
  - **Auth fix**: JRK's patched `start_channels()` now passes `header={auth_header_key: auth_scheme + auth_token}` to `create_connection()`. `KernelChannelsHandler` uses standard `@authenticated`.
  - **Upstream fix still needed**: `jupyter_server`'s `GatewayKernelClient.start_channels()` should pass auth headers and use `run_in_executor` to avoid deadlock when the gateway is in the same process.
- **Missing `/api/sessions` endpoint**: Hub does not implement `/api/sessions`. Some GatewayClient versions may proxy sessions through the gateway; if so, the 404 could prevent proper session-kernel binding and code execution.
- **Multiple JupyterLab processes on same port**: If JupyterLab is not cleanly killed (e.g., `kill` sent but process lingers), subsequent starts silently bind to next available port (8891, 8892...) while `GatewayClient.url` still points to 8890. Always verify with `ps aux | grep jupyterlab` and kill all stale processes before restarting.
