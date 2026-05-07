# CLAUDE.md — jupyter-remote-kernel

## Project Overview

A reverse WebSocket tunnel that exposes Jupyter kernels from remote machines (behind NAT/firewalls) to a local JupyterLab via the Jupyter Gateway API.

**两种部署模式**：
- **Extension 模式**（推荐）：Hub 作为 Jupyter Server Extension 嵌入 JupyterLab，共享 auth
- **Standalone 模式**：Hub 作为独立进程运行（适合部署在公网服务器）

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
JupyterLab (本地)
  └── GatewayClient
        │  HTTP/WS
        ▼
      Hub                            ← Jupyter Gateway API 兼容
      (嵌入 JupyterLab 或独立进程)
        │  WebSocket 隧道（单连接多路复用）
        ▼
      RemoteAgent (agent.py)         ← 远端无公网机器
        │  本地 HTTP/WS
        ▼
      Jupyter Server (远端本地)
```

### Extension 模式特点

- Hub 端点挂在 `/jrk/` 路径下（如 `/jrk/api/kernels`）
- JupyterLab 的 `@authenticated` 装饰器自动保护所有端点
- `GatewayClient.url=http://localhost:<port>/jrk` 指向自身（非循环：browser → JupyterLab server → extension handler → agent）
- Agent 连接时用 `Authorization: token <jupyterlab-token>` header 认证

### Standalone 模式特点

- Hub 独立监听端口，支持 `--token` 选项
- Agent 通过 register 消息中的 `token` 字段认证
- API 端点通过 `Authorization: token <token>` header 认证（aiohttp middleware）

## Gateway API Compliance

GatewayClient 调用的全部接口及实现状态：

### KernelSpec 接口

| 接口 | 状态 |
|------|------|
| `GET /api/kernelspecs` | ✅ 已实现 |
| `GET /api/kernelspecs/{kernel_name}` | ✅ 已实现 |
| `GET /kernelspecs/{kernel_name}/{resource}` | ✅ 已实现 |

### Kernel 接口

| 接口 | 状态 |
|------|------|
| `GET /api/kernels` | ✅ 已实现 |
| `POST /api/kernels` | ✅ 已实现 |
| `GET /api/kernels/{kernel_id}` | ✅ 已实现 |
| `DELETE /api/kernels/{kernel_id}` | ✅ 已实现 |
| `POST /api/kernels/{kernel_id}/restart` | ✅ 已实现 |
| `POST /api/kernels/{kernel_id}/interrupt` | ✅ 已实现 |
| `WS /api/kernels/{kernel_id}/channels` | ✅ 已实现 |

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
- Agent generates a random token for its local Jupyter Server. On restart, it kills any existing process on the port to avoid token mismatch.
- Agent passes `cwd=root_dir` to subprocess — `--ServerApp.root_dir` only affects the file browser, not the kernel's cwd.
- Agent sends both `Authorization` header (for extension mode) AND `token` field in register message (for standalone mode).

## Build & Run

```bash
pip install -e .

# Extension mode (recommended)
jupyter lab --port=8890 --ServerApp.token=test \
  --GatewayClient.url=http://localhost:8890/jrk \
  --GatewayClient.auth_token=test

# Agent (connects to extension mode)
jupyter-remote-kernel agent --hub http://localhost:8890/jrk --name test --token test

# Standalone mode
jupyter-remote-kernel hub --port 8765 --token my-secret
jupyter-remote-kernel agent --hub http://hub-host:8765 --name gpu-machine --token my-secret

# JupyterLab connects to standalone hub
jupyter lab --GatewayClient.url=http://hub-host:8765 --GatewayClient.auth_token=my-secret
```

## Dependencies

- `aiohttp>=3.9` — async HTTP server/client (standalone Hub + agent's WS client)
- `jupyter_server>=2.0` — remote Jupyter Server (started by agent) + extension framework (Tornado handlers)
- `ipykernel` — must be installed on remote machines
