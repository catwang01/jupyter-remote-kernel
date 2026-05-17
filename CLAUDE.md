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
tests/
├── conftest.py          # Subprocess fixtures: JupyterLab + agents
├── helpers.py           # Shared: AUTH, JRK, poll_kernel_idle, ws_execute
├── test_kernelspecs.py  # Kernelspec list/single/name/default
├── test_kernels.py      # Kernel CRUD + aggregation
├── test_execution.py    # WS code execution (print, expression, error)
├── test_routing.py      # Agent routing isolation + prefix routing
├── test_reconnect.py    # Agent disconnect/reconnect behaviour
├── test_errors.py       # Error paths (duplicate name, 404s)
└── test_kernel_lifecycle.py  # Kernel lifecycle (death, delete-when-agent-gone)
jupyter-config/
└── server_config.d/
    └── jupyter_remote_kernel.json  # Auto-enables extension on install
pyproject.toml           # Build system only (setuptools>=40)
setup.cfg                # Package metadata (name, version, deps, entry points)
pytest.ini               # asyncio_mode=auto, timeout=120
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
4. **Agent name uniqueness**: Hub rejects duplicate agent names. If an agent tries to register with a name already in use, registration fails with error message and agent exits. Prevents kernel routing conflicts.
5. **Files stay local** — JupyterLab manages files locally. Only kernel execution happens remotely.
6. **Auth sharing** — Extension mode reuses JupyterLab's token; standalone mode uses its own `--token` flag.
7. **Auto port selection** — Agent finds a free port automatically (`socket.bind(0)`), no manual `--jupyter-port` needed.
8. **Auto agent name** — If `--name` is omitted, agent defaults to `{hostname}-{os}-{arch}` (e.g., `mypc-windows-amd64`). Uses `platform.node()`, `platform.system()`, `platform.machine()`.

## Important Implementation Notes

- `api_kernelspecs` `"default"` field must be a string (not null) — JupyterLab's GatewayMappingKernelManager expects unicode. Hub uses `next(iter(specs), "")` so it returns the first kernelspec name if agents are connected, or empty string if none.
- **Kernelspec `name` field bug (FIXED 2026-05-14)**: Hub must set `v["name"] = key` (e.g., `machine-1:python3`) in the kernelspec response. Without this, JupyterLab sends just `"python3"` when creating kernels, and Hub routes all to the first tunnel. Fixed in both `KernelSpecsHandler.get()` (list) and `KernelSpecHandler.get()` (single).
- **`POST /api/kernels` returns 201**: Hub proxies the status code from the remote Jupyter Server, which returns 201 Created (not 200). Clients should accept both `200` and `201`.
- **Tornado `finish()` rejects lists**: `KernelsHandler.get` aggregates results from all agents into a list. Tornado's `finish()` refuses to serialize lists directly (CSRF protection). Must use `self.finish(json.dumps(results))` with explicit `Content-Type: application/json` header.
- Agent generates a random token for its local Jupyter Server. On restart, it kills any existing process on the port to avoid token mismatch.
- Agent passes `cwd=root_dir` to subprocess — `--ServerApp.root_dir` only affects the file browser, not the kernel's cwd.
- Agent automatically culls idle kernels (1 hour timeout, checks every 5 minutes) to clean up stale kernel processes. Override with `-- --MappingKernelManager.cull_idle_timeout=0` to disable.
- Hub runs `prune_stale_kernels()` every 5 minutes: queries each connected agent's `/api/kernels`, removes `kernel_tunnel` entries for kernels that no longer exist on the remote side. Uses aiohttp `on_startup` background task in standalone mode and `tornado.ioloop.PeriodicCallback` in extension mode.
- Agent prints heartbeat messages every 10 seconds showing kernel count and execution states (e.g., `[Agent] Heartbeat: 3 kernel(s) running (2 idle, 1 busy)`). Queries local Jupyter Server's `/api/kernels` endpoint.
- Agent sends both `Authorization` header (for extension mode) AND `token` field in register message (for standalone mode).
- Agent supports `--extra-header Key:Value` (repeatable) for custom headers on all Hub requests (e.g., Cloudflare Access, custom API gateways). Extra headers are merged after the `Authorization` header, so they can also override it.
- Agent supports passing extra arguments to Jupyter Server via `--` separator. All arguments after `--` are appended to the `jupyter_server` command (e.g., `agent --hub URL --name foo -- --ServerApp.allow_origin='*'`). Useful for custom Jupyter Server configurations without modifying agent code.
- **Reverse proxy path**: When JupyterLab runs with `--ServerApp.base_url=/jupyter`, the Hub endpoint becomes `/jupyter/jrk/`. Agents must use the full path (e.g., `--hub http://host/jupyter/jrk`), not just `/jrk`.
- **Tunnel heartbeat**: Agent uses `heartbeat=30.0` on the tunnel WS (`aiohttp` ping/pong) to prevent idle connection resets. Without this, the server may close the tunnel due to inactivity, causing `Connection reset by peer` (errno 54 on macOS).
- **Kernel restore on reconnect**: When an agent disconnects, `on_close()` removes all its kernel mappings from `kernel_tunnel`. On re-registration, `_restore_kernels()` queries `GET /api/kernels` on the agent's local Jupyter Server and re-populates the mappings. Without this, existing kernels become unreachable after agent reconnect, causing "Lost connection to Gateway" loops.
- **Kernel channels WS auth**: `GatewayKernelClient.start_channels()` (from `jupyter_server`) does not pass auth headers — upstream bug. JRK's patched `_start_channels_nonblocking` fixes this by passing `{auth_header_key: auth_scheme + auth_token}` explicitly in `create_connection()`. `KernelChannelsHandler` uses standard `@authenticated`.
- **Browser WS goes through JupyterLab proxy, not directly to jrk**: Browser connects to `ws://host/api/kernels/{id}/channels` (without `/jrk/`). JupyterLab's server-side gateway proxy then connects to `ws://localhost/jrk/api/kernels/{id}/channels` internally. Browser never directly accesses `/jrk/` endpoints. When accessing behind a reverse proxy (Cloudflare/nginx/frp), WS failures are caused by JupyterLab's own auth mechanism, not JRK's.
- **Notebook kernelspec metadata**: Notebooks must have both `name` and `display_name` in `metadata.kernelspec`. Missing `display_name` causes repeated `Notebook JSON is invalid` errors on save (though it does not block execution directly).
- **Extension loading: `__init__.py` is critical**: `jupyter_server` imports the top-level package (`jupyter_remote_kernel`), NOT `server_extension.py` directly. So `__init__.py` MUST re-export `_jupyter_server_extension_points` from `server_extension.py`. Without this, extension loading fails with `_load_jupyter_server_extension function was not found`.
- **Agent starts Jupyter via `python3 -m jupyter_server`**: NOT `python3 -m jupyter server`. The latter relies on `jupyter` dispatching to a `jupyter-server` script in PATH, which fails on systems where user Python bin is not in PATH (e.g., macOS Xcode Python 3.9 with user packages at `~/Library/Python/3.9/bin`).
- **Agent `--allow-root`**: The agent's local Jupyter Server command includes `--allow-root` to support running as root (e.g., on cloud servers). Without this, Jupyter Server refuses to start and the agent times out after 30s.
- **Agent hub URL must use public hostname**: When the agent runs on the same server as frps (e.g., Aliyun), it must connect to the hub via the public hostname (e.g., `http://catwang.top/jupyter/jrk`), NOT `http://localhost/...`. Connecting via localhost hits frps's bound port but results in 404 because the loopback path doesn't proxy correctly.
- **Auto-enable doesn't work with editable install**: `pip install -e .` does NOT install `data_files`. Must manually copy `jupyter-config/server_config.d/jupyter_remote_kernel.json` to `<prefix>/etc/jupyter/jupyter_server_config.d/`, or run `jupyter server extension enable jupyter_remote_kernel`.
- **Multiple Python installs on remote machines**: When deploying the agent, the `jupyter-remote-kernel` binary's shebang determines which Python runs it. If a machine has multiple Pythons (e.g., system Python 3.9, Homebrew Python 3.13, Conda), you must `pip install` using the same Python that is in PATH. Verify with `head -1 $(which jupyter-remote-kernel)`.
- **Windows deployment (JD Cloud)**: Agent runs on Windows with miniconda Python 3.13. Install via wheel transfer (`scp` + `pip install --no-index`) due to socket exhaustion from `xtquant_server`. The server has ~5985 SYN_SENT connections to `127.0.0.1:58610` consuming ephemeral ports, causing intermittent `WinError 10055` (WSAENOBUFS). Agent itself only needs 1 persistent WS connection so this doesn't block operation once started. Use `.ps1` script files for complex PowerShell commands via SSH (multiline quoting issues). Auto-start configured via `schtasks` (Windows Task Scheduler) with task name `JupyterRemoteKernelAgent`, `python.exe -u C:\Users\administrator\run_agent.py`. GUI apps (e.g., 同花顺/hexin.exe) also use `schtasks` — NSSM is unsuitable for GUI apps as Windows Services run in Session 0 (isolated from desktop).
- **Windows agent requires wrapper script**: `python -m jupyter_remote_kernel` fails because the package has no `__main__.py`. On Windows, use a wrapper script (`run_agent.py`) that sets `sys.argv`, redirects stdout/stderr to a log file (line-buffered), and calls `from jupyter_remote_kernel.cli import main; main()`. Log redirection must be done in-process because `Start-Process -RedirectStandardOutput` produces empty logs.
- **SSH + Windows Job Object kills child processes**: On Windows, SSH creates a Job Object that kills all descendant processes when the session ends. `Start-Process` and `pythonw.exe` do NOT escape this. Only `schtasks` (Task Scheduler) creates truly independent processes that survive SSH disconnection.
- **Agent `--debug` flag**: Prints all tunnel WebSocket messages (`[DEBUG] RECV/SEND`) with type, req_id/ws_id, and complete data/body fields. Useful for verifying messages flow bidirectionally.
- **GatewayKernelClient monkey-patches** (in `server_extension.py`): Two patches applied at module load time to fix compatibility with `jupyter-server-nbmodel`'s `POST /api/kernels/{id}/execute` endpoint:
  1. **`start_channels()` deadlock + auth fix**: Original `start_channels()` calls `websocket.create_connection()` synchronously (deadlock) and without auth headers (403). Fix: run in `loop.run_in_executor()` and pass `{auth_header_key: auth_scheme + auth_token}` in `header=`. After connection, `ws.settimeout(None)` removes the launch timeout from recv — without this, `response_router` dies after `KERNEL_LAUNCH_TIMEOUT` (40s) of no messages, silently killing the kernel channel. The `response_router` thread is wrapped in `_guarded_route_responses` which logs `[JRK] response_router died` at ERROR level if it exits unexpectedly (the original thread dies silently with no diagnostics).
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

# Agent without --name (defaults to {hostname}-{os}-{arch})
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --token test

# Agent with debug logging (prints all tunnel messages)
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name test --token test --debug

# Agent with extra headers (e.g., behind Cloudflare Access)
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name test --token test \
  --extra-header "CF-Access-Client-Id:abc" --extra-header "CF-Access-Client-Secret:xyz"

# Agent with custom Jupyter Server arguments (passed after --)
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name test --token test \
  -- --ServerApp.allow_origin='*' --ServerApp.terminals_enabled=False

# Standalone mode
jupyter-remote-kernel hub --port 8765 --token my-secret
jupyter-remote-kernel agent --hub http://hub-host:8765 --name gpu-machine --token my-secret

# JupyterLab connects to standalone hub
jupyter lab --GatewayClient.url=http://hub-host:8765 --GatewayClient.auth_token=my-secret

# With base_url (e.g., behind reverse proxy)
# Agent must include the full base_url path:
jupyter-remote-kernel agent --hub http://host/jupyter/jrk --name test --token <token>

# Aliyun agent (systemd service on catwang.top, connects to myjupyterlab hub)
# Service: /etc/systemd/system/jupyter-remote-kernel-agent.service
# Install: /opt/jupyter-remote-kernel/venv/
jupyter-remote-kernel agent --hub http://catwang.top/jupyter/jrk --name aliyun --token <token>

# JD Cloud agent (Windows, 117.72.145.38, user: administrator)
# Python: miniconda 3.13 at C:\ProgramData\miniconda3\python.exe
# Binary: C:\ProgramData\miniconda3\Scripts\jupyter-remote-kernel.exe
# Install: scp wheel + pip install --no-index (socket exhaustion blocks PyPI)
# Auto-start: schtasks (Task Scheduler), task name "JupyterRemoteKernelAgent", triggers on logon
jupyter-remote-kernel agent --hub http://catwang.top/jupyter/jrk --name jd-cloud --token <token>

# localmac agent (this Mac, connects directly to myjupyterlab container, no frp)
# PID-based process — not in Docker, not systemd. Start manually or via launchctl.
jupyter-remote-kernel agent --hub http://localhost:8888/jupyter/jrk --name localmac --token <token>

# edmac agent (Ed's Mac)
jupyter-remote-kernel agent --hub http://catwang.top/jupyter/jrk --name edmac --token <token>
```

### Deploying to myjupyterlab (production)

Source code is volume-mounted into the `myjupyterlab` container via `~/jupyterlab/docker-compose.yaml`:
```
${PWD}/workspace/jupyter-remote-kernel/src/jupyter_remote_kernel → /opt/conda/lib/python3.10/site-packages/jupyter_remote_kernel
```

After code changes, restart the container to apply:
```bash
cd ~ && bash run.sh up
```

No wheel build or `pip install` needed — the mount makes the host source visible inside the container immediately.

## Testing

Integration tests run in extension mode: a JupyterLab process with the JRK extension + two agent subprocesses, all managed by pytest session-scoped fixtures.

```bash
# Install test deps
pip install -e ".[test]"

# Run all tests (~5 minutes)
pytest tests/

# Run a single file
pytest tests/test_kernels.py -v
```

**Test architecture**:
- JupyterLab on `localhost:18890` with `token=test`, `GatewayClient.url=http://localhost:18890/jrk`
- Two agents (`agent1`, `agent2`) as session-scoped fixtures; `kernel` fixture is function-scoped (creates + deletes per test via `/jrk/api/kernels`); `kernel_via_jlab` fixture creates via JupyterLab's native `/api/kernels` (GatewayClient proxy path, same as MCP/browser)
- `ws_execute()` helper supports two modes: direct JRK WS (`/jrk/api/kernels/{id}/channels`) or via JupyterLab proxy (`/api/kernels/{id}/channels`, triggered by `via_jlab=True`)
- `pytest-asyncio` with `asyncio_mode=auto`; `pytest-timeout` at 120s per test

**26 tests across 7 files**:
- `test_kernelspecs.py` (4) — list, single, name field, default type
- `test_kernels.py` (7) — create, get, list, aggregation across agents, delete, restart, interrupt
- `test_execution.py` (4) — print, expression, error, long-running via JupyterLab proxy (5s sleep regression)
- `test_routing.py` (2) — agent routing isolation, prefix routing
- `test_reconnect.py` (2) — agent disconnect cleanup, reconnect + new kernel
- `test_errors.py` (3) — duplicate agent name, nonexistent kernel GET/DELETE
- `test_kernel_lifecycle.py` (4) — hub delete from registry, kernel death, delete when agent gone, WS to deleted kernel (close code 4404)

## Dependencies

- `aiohttp>=3.6` — async HTTP server/client (standalone Hub + agent's WS client). Only required pip dependency.
- `jupyter_server>=1.0` — optional (`pip install .[hub]`), needed for extension mode Hub. Agent starts it as a subprocess (not imported).
- `ipykernel` — must be installed on remote machines (agent's Jupyter Server subprocess needs it)

**Python 3.6 compatibility**: Agent and CLI use only `asyncio.ensure_future()` and `get_event_loop().run_until_complete()` (no `asyncio.create_task` or `asyncio.run`). Metadata lives in `setup.cfg` (not `pyproject.toml` `[project]` table) so older setuptools (≤59) can build it.

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
- **Kernel stuck in "starting" after creation**: When `POST /api/kernels` succeeds (kernel ID returned), the kernel may remain in `execution_state: "starting"` with `connections: 0` indefinitely. The remote agent's local Jupyter Server created the kernel entry but the `ipykernel` process didn't fully initialize. **Fix**: `POST /api/kernels/{id}/restart` — this reliably transitions the kernel to `idle`. Root cause unclear (likely race condition in ipykernel startup on the remote machine). Observed on `local-machine` agent (2026-05-13).
- **Stale kernel accumulation in `hub_state.kernel_tunnel` (FIXED 2026-05-15)**: `_restore_kernels()` now reconciles on agent reconnect (removes stale entries for that agent + adds missing ones). Both `hub.py` and `server_extension.py` also run a periodic `prune_stale_kernels()` every 5 minutes: for each connected agent, queries `/api/kernels` and removes entries no longer alive. The periodic task is started via aiohttp `on_startup` in standalone mode and `tornado.ioloop.PeriodicCallback` in extension mode.
- **`on_close()` queue/future cleanup (FIXED 2026-05-13)**: When an agent's tunnel WebSocket disconnects, `on_close()` now drains `_ws_queues` with `None` sentinels, rejects pending `_http` and `_ws_open` futures with `ConnectionError("tunnel closed")`, then clears all dicts. Fixed in both `server_extension.py` and `hub.py`. Previously, `_relay_from_remote` tasks hung forever on `queue.get()`, causing "Lost connection to Gateway" loops.
- **frp tunnel instability → 502/Server disconnected**: Aliyun agent connects through frps → frpc → mynginx → myjupyterlab. When the frp TCP tunnel between frps and frpc drops (network jitter, idle timeout), nginx returns 502 Bad Gateway. The agent sees alternating `Server disconnected` and `502 Invalid response status` errors. Agent auto-reconnects (5s retry), but each drop causes all active kernel WS channels to die. nginx `proxy_read_timeout: 120s` is in `jupyterlab.conf`; agent heartbeat is 30s — sufficient for nginx but not for frp's own tunnel timeout settings.
