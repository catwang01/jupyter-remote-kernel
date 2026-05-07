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
- `--name NAME` — Display name, used as kernel prefix in JupyterLab (required)
- `--token TOKEN` — Auth token (required if Hub/JupyterLab has token auth)
- `--jupyter-port PORT` — Local Jupyter Server port (default: auto-selects free port)
- `--root-dir DIR` — Kernel working directory (created if absent)

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

1. **Duplicate agent names**: If two agents register with the same name, the second overwrites the first. When either disconnects, the name is removed.

2. **macOS firewall blocks background processes**: On macOS, `nohup` processes may be blocked from outbound connections. Workaround: SSH reverse port forwarding (`ssh -f -N -R <port>:localhost:<port> user@remote`).

3. **Agent restart requires port cleanup**: The agent kills any existing process on its Jupyter port before starting to avoid token mismatch.

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
