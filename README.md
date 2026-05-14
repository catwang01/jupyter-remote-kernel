# jupyter-remote-kernel

Run Jupyter kernels on remote machines through a reverse WebSocket tunnel. No inbound connectivity required — works through NAT and firewalls.

## Architecture

```
┌─────────────┐         ┌─────────────┐         ┌─────────────────────┐
│ JupyterLab  │◄─HTTP──►│     Hub     │◄─WS────►│   Remote Agent      │
│  (browser)  │         │ (Gateway)   │  tunnel  │                     │
│             │         │             │          │  ┌───────────────┐  │
│  files live │         │             │          │  │Jupyter Server │  │
│  here       │         │             │          │  │  (kernels)    │  │
└─────────────┘         └─────────────┘          │  └───────────────┘  │
                                                 └─────────────────────┘
                                                   Behind NAT/firewall
```

**Key design**: The remote agent initiates an **outbound** WebSocket connection to the Hub. The Hub exposes a Jupyter Gateway-compatible API, so JupyterLab treats it as a standard kernel gateway. All kernel traffic (HTTP + WebSocket) is multiplexed over the single tunnel connection.

**Files vs Kernels**: Files are created and managed in JupyterLab (local). Only kernel execution happens on the remote machine.

## Installation

```bash
pip install -e .
```

Requirements: Python >= 3.9, `aiohttp >= 3.9`, `jupyter_server >= 2.0`. Remote machines also need `ipykernel` installed.

## Deployment Modes

### Mode A: Embedded in JupyterLab (recommended)

The Hub runs as a Jupyter Server extension inside JupyterLab. No separate Hub process needed. Auth is shared with JupyterLab's own token.

```bash
# Start JupyterLab (Hub is built-in)
jupyter lab \
  --ip=0.0.0.0 --port=8890 \
  --ServerApp.token=mytoken \
  --GatewayClient.url=http://localhost:8890/jrk \
  --GatewayClient.auth_token=mytoken

# Connect agents (use JupyterLab's token)
jupyter-remote-kernel agent \
  --hub http://<jupyterlab-ip>:8890/jrk \
  --name gpu-machine \
  --token mytoken
```

The extension auto-enables on install. Endpoints are mounted at `/jrk/`:
- `/jrk/tunnel/register` — agent WebSocket tunnel
- `/jrk/api/kernelspecs` — aggregated kernelspecs
- `/jrk/api/kernels` — kernel CRUD
- `/jrk/api/kernels/{id}/channels` — kernel WebSocket
- `/jrk/debug/tunnels` — debug info

### Mode B: Standalone Hub

Run the Hub as a separate process. Useful when deploying on a public server or when Hub and JupyterLab are on different machines.

```bash
# Start Hub (public server)
jupyter-remote-kernel hub --port 9100 --token secret123

# Connect agents
jupyter-remote-kernel agent \
  --hub http://<hub-ip>:9100 \
  --name gpu-machine \
  --token secret123

# Connect JupyterLab
jupyter lab \
  --GatewayClient.url=http://<hub-ip>:9100 \
  --GatewayClient.auth_token=secret123
```

## Agent Setup

On each remote machine:

```bash
# 1. Install
pip install jupyter-remote-kernel ipykernel

# 2. Start agent (minimal)
jupyter-remote-kernel agent --hub http://<hub-url> --name my-machine --token <token>
```

Options:
- `--hub URL` — Hub endpoint (required)
- `--name NAME` — Display name, used as kernel prefix in JupyterLab (required). Must be unique across all connected agents
- `--token TOKEN` — Auth token (required if Hub/JupyterLab has token auth)
- `--jupyter-port PORT` — Local Jupyter Server port (default: auto-selects free port)
- `--root-dir DIR` — Kernel working directory (created if absent)
- `--debug` — Print all tunnel WebSocket messages to stdout (useful for troubleshooting)
- `--extra-header KEY:VALUE` — Extra HTTP header added to all Hub requests (repeatable, useful for reverse proxies like Cloudflare Access)
- `-- <args>` — Pass extra arguments to the underlying Jupyter Server (e.g., `-- --ServerApp.allow_origin='*'`)

## Authentication

- **Mode A (extension)**: Hub reuses JupyterLab's own token. Anyone who can access JupyterLab can use remote kernels. Agent must pass the same token.
- **Mode B (standalone)**: Hub accepts `--token`. When set, both agents and JupyterLab must supply it. When unset, no auth (not recommended for production).

## Protocol

The tunnel uses a single WebSocket connection per agent, multiplexing HTTP and WebSocket traffic:

### Hub → Agent

| Message | Fields | Purpose |
|---------|--------|---------|
| `http_req` | `req_id, method, path, headers, body` | Proxy HTTP request |
| `ws_open` | `ws_id, path` | Open kernel WebSocket |
| `ws_frame` | `ws_id, data, binary` | Forward WebSocket frame |
| `ws_close` | `ws_id` | Close kernel WebSocket |

### Agent → Hub

| Message | Fields | Purpose |
|---------|--------|---------|
| `http_res` | `req_id, status, headers, body` | HTTP response |
| `ws_opened` | `ws_id` | WebSocket ready |
| `ws_reject` | `ws_id, error` | WebSocket failed |
| `ws_frame` | `ws_id, data, binary` | Forward WebSocket frame |
| `ws_close` | `ws_id` | WebSocket closed |

Binary WebSocket data is base64-encoded in the JSON envelope.

## Debugging

```bash
# Mode A (extension)
curl -H "Authorization: token mytoken" http://localhost:8890/jrk/debug/tunnels

# Mode B (standalone)
curl http://localhost:9100/debug/tunnels
```

## Known Issues

1. **Duplicate agent names**: Hub rejects registration if an agent with the same name is already connected. The second agent exits with an error message.

2. **macOS firewall blocks background processes**: On macOS, `nohup` processes may be blocked from outbound connections. Workaround: SSH reverse port forwarding (`ssh -f -N -R <port>:localhost:<port> user@remote`).

3. **Agent restart requires port cleanup**: The agent kills any existing process on its Jupyter port before starting to avoid token mismatch.

4. **Reverse proxy base_url**: When JupyterLab runs with a `base_url` (e.g., `--ServerApp.base_url=/jupyter`), the agent must include the full path: `--hub http://host/jupyter/jrk`. Using just `/jrk` without the base_url prefix results in 404.

5. **Kernel stuck in "starting" after creation**: A newly created kernel may remain in `starting` state with 0 connections indefinitely. The `ipykernel` process on the remote machine failed to fully initialize. Fix: restart the kernel via `POST /api/kernels/{id}/restart` — this reliably transitions it to `idle`.

6. **Tunnel disconnect and kernel channel hang** (fixed): When the agent's tunnel WebSocket drops (frp instability, network timeout, agent restart), existing kernel channels previously hung indefinitely because `on_close()` did not drain pending queues/futures. Now fixed: `on_close()` sends `None` sentinels to all WS queues and rejects all pending HTTP/WS-open futures with `ConnectionError`, so relay tasks terminate immediately. Kernel restore still happens automatically on agent re-registration.

7. **frp tunnel instability**: When using frp (fast reverse proxy) as the transport layer, the tunnel WebSocket may receive 502 Bad Gateway or "Server disconnected" errors during frps/frpc restarts or network hiccups. The agent auto-reconnects with a 5-second backoff, and kernels are restored on re-registration.

## Development

```bash
# Install in editable mode
pip install -e .

# Test locally (extension mode)
jupyter lab --port=8890 --ServerApp.token=test \
  --GatewayClient.url=http://localhost:8890/jrk \
  --GatewayClient.auth_token=test

# In another terminal
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name local-test --token test
```

## License

MIT
