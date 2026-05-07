# CLAUDE.md — jupyter-remote-kernel

## Project Overview

A reverse WebSocket tunnel that exposes Jupyter kernels from remote machines (behind NAT/firewalls) to a local JupyterLab via the Jupyter Gateway API.

**Two deployment modes**:
- **Extension mode** (recommended): Hub embedded in JupyterLab as a Jupyter Server Extension, shares auth
- **Standalone mode**: Hub runs as a separate process (suitable for public servers)

## File Structure

```
src/jupyter_remote_kernel/
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
- JupyterLab's `@authenticated` decorator protects all endpoints automatically
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

## Build & Run

```bash
pip install -e .

# Extension mode (recommended)
jupyter lab --port=8890 --ServerApp.token=test \
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
