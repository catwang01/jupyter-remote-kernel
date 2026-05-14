# Integration Test Suite Design

**Date**: 2026-05-14  
**Scope**: Automated integration tests for `jupyter-remote-kernel` extension mode

---

## Goal

A `pytest` suite that spins up a real JupyterLab (extension mode) plus two real agents, exercises all major code paths, and asserts on actual returned values. Runs locally with `pytest tests/` and is suitable for CI given sufficient timeout budgets.

---

## Architecture

### Deployment under test

```
JupyterLab (port 18890, token=test)
  └── JRK server extension at /jrk/
        ├── agent1 tunnel  (agent name: "agent1")
        └── agent2 tunnel  (agent name: "agent2")
```

Tests call `/jrk/api/...` directly with `Authorization: token test`. They do **not** go through JupyterLab's GatewayClient proxy — this isolates JRK logic from JupyterLab's own WS auth layer.

### File structure

```
tests/
  conftest.py           # session-scoped subprocess fixtures
  helpers.py            # wait_for_ready, ws_execute, poll_kernel_idle
  test_kernelspecs.py
  test_kernels.py
  test_execution.py
  test_routing.py
  test_reconnect.py
  test_errors.py
```

---

## Fixtures (`conftest.py`)

### `jupyterlab` (session-scoped)

Starts JupyterLab as a subprocess:

```
python3 -m jupyterlab --no-browser --ip=127.0.0.1 --port=18890
  --ServerApp.token=test
  --GatewayClient.url=http://localhost:18890/jrk
  --GatewayClient.auth_token=test
```

Ready check: `GET /jrk/api/kernelspecs` with auth, poll every 1s, timeout 60s.  
Yields: `{"base_url": "http://localhost:18890", "token": "test"}`  
Teardown: `proc.terminate()` + `proc.wait(timeout=10)`

### `agent1`, `agent2` (session-scoped, depend on `jupyterlab`)

Each starts:

```
jupyter-remote-kernel agent
  --hub http://localhost:18890/jrk
  --name agent1   # or agent2
  --token test
```

Ready check: `GET /jrk/debug/tunnels`, poll every 1s, timeout 30s, confirm agent name appears in JSON.  
Yields: agent name string  
Teardown: `proc.terminate()` + `proc.wait(timeout=10)`

---

## Helper functions (`helpers.py`)

### `wait_for_http(url, headers, timeout, interval) -> None`

Polls `GET url` until HTTP 200. Raises `TimeoutError` on expiry.

### `poll_tunnel_registered(base_url, token, agent_name, timeout) -> None`

Polls `GET /jrk/debug/tunnels` until `agent_name` appears in the response JSON.

### `poll_kernel_idle(base_url, token, kernel_id, timeout) -> None`

Polls `GET /jrk/api/kernels/{kernel_id}` until `execution_state == "idle"`.  
If not idle within timeout, sends `POST /jrk/api/kernels/{kernel_id}/restart` once and waits again.  
(Handles known race condition in ipykernel startup on remote machines.)

### `ws_execute(ws_url, token, code) -> dict`

Opens a WebSocket to `/jrk/api/kernels/{id}/channels` (with auth header), sends an `execute_request` message following the Jupyter Wire Protocol, collects `stream` / `execute_result` / `execute_reply` messages keyed by `parent_header.msg_id`, returns:

```python
{"output": str, "status": "ok" | "error", "execution_count": int}
```

---

## Test scenarios

### `test_kernelspecs.py`

| Test | What it checks |
|------|----------------|
| `test_list_kernelspecs` | Response has keys `agent1:python3` and `agent2:python3` |
| `test_kernelspec_name_field` | `v["name"] == key` for each kernelspec (routing correctness) |
| `test_default_is_empty_string` | `response["default"] == ""` (not null — JupyterLab requirement) |
| `test_get_single_kernelspec` | `GET /jrk/api/kernelspecs/agent1:python3` returns 200 |

### `test_kernels.py`

| Test | What it checks |
|------|----------------|
| `test_create_kernel` | `POST /jrk/api/kernels` with `{"name": "agent1:python3"}` returns 200 with `id` field |
| `test_get_kernel` | `GET /jrk/api/kernels/{id}` returns `id`, `name`, `execution_state` |
| `test_list_kernels` | `GET /jrk/api/kernels` returns a list containing the created kernel |
| `test_delete_kernel` | `DELETE /jrk/api/kernels/{id}` returns 204; subsequent GET returns 404 |
| `test_restart_kernel` | `POST /jrk/api/kernels/{id}/restart` returns 200 |
| `test_interrupt_kernel` | `POST /jrk/api/kernels/{id}/interrupt` returns 204 |

### `test_execution.py`

| Test | What it checks |
|------|----------------|
| `test_execute_print` | `print("hello")` → `stream.text == "hello\n"`, `execute_reply.status == "ok"` |
| `test_execute_expression` | `1 + 1` → `execute_result.data["text/plain"] == "2"` |
| `test_execute_error` | `raise ValueError("boom")` → `execute_reply.status == "error"`, `ename == "ValueError"` |

### `test_routing.py`

| Test | What it checks |
|------|----------------|
| `test_kernels_route_to_different_agents` | Create kernel on agent1 and agent2; execute `import os; print(os.getpid())` on each; assert PIDs differ (different ipykernel processes) |
| `test_kernelspec_prefix_routing` | Kernel named `agent2:python3` is created on agent2, not agent1 |

### `test_reconnect.py`

| Test | What it checks |
|------|----------------|
| `test_agent_disconnect_cleans_state` | Kill agent1 process; `GET /jrk/debug/tunnels` no longer contains `agent1` |
| `test_agent_reconnect_restores_kernels` | Create kernel on agent1; kill agent1; restart agent1; wait for re-registration; `GET /jrk/api/kernels/{id}` still returns the kernel |

### `test_errors.py`

| Test | What it checks |
|------|----------------|
| `test_duplicate_agent_name_rejected` | Start a third agent with `name=agent1`; assert its process exits with non-zero code within 10s |
| `test_get_nonexistent_kernel` | `GET /jrk/api/kernels/does-not-exist` returns 404 |
| `test_delete_nonexistent_kernel` | `DELETE /jrk/api/kernels/does-not-exist` returns 404 |

---

## Dependencies

Add to `pyproject.toml`:

```toml
[project.optional-dependencies]
test = ["pytest>=8", "pytest-asyncio>=0.24"]
```

`aiohttp` is already a project dependency — use its `ClientSession` and WS client throughout. No additional WS library needed.

Install: `pip install -e ".[test]"`  
Run: `pytest tests/ -v --timeout=120`

---

## Key constraints

- **Port 18890** is hardcoded in fixtures to avoid conflicts with development JupyterLab (8890).
- Tests are **not order-dependent**: each test that needs a kernel creates its own and deletes it in teardown (function-scoped kernel fixture).
- `test_reconnect.py` uses a **function-scoped agent fixture** (separate from the session-scoped one) so it can kill/restart without affecting other tests.
- All async tests use `@pytest.mark.asyncio` with `asyncio_mode = "auto"` in `pytest.ini`.
