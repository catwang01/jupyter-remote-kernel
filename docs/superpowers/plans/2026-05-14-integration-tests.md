# Integration Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a pytest integration test suite that starts a real JupyterLab (extension mode) + two real agents and verifies all major JRK code paths.

**Architecture:** Session-scoped subprocess fixtures start JupyterLab (port 18890) and two `jupyter-remote-kernel agent` subprocesses. Tests call `/jrk/api/...` directly with auth headers. Async tests use `pytest-asyncio`. A `kernel` function-scoped fixture creates/deletes kernels per test.

**Tech Stack:** `pytest>=8`, `pytest-asyncio>=0.24`, `aiohttp` (already a project dep), Python `subprocess` + `asyncio`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `pyproject.toml` | Modify | Add `[project.optional-dependencies] test = [...]` |
| `pytest.ini` | Create | `asyncio_mode = auto`, timeout config |
| `tests/__init__.py` | Create | Empty, makes `tests/` a package |
| `tests/helpers.py` | Create | `wait_for_http`, `poll_tunnel_registered`, `poll_kernel_idle`, `ws_execute` |
| `tests/conftest.py` | Create | `jupyterlab`, `agent1`, `agent2`, `kernel` fixtures |
| `tests/test_kernelspecs.py` | Create | 4 kernelspec tests |
| `tests/test_kernels.py` | Create | 6 kernel CRUD tests |
| `tests/test_execution.py` | Create | 3 WS code-execution tests |
| `tests/test_routing.py` | Create | 2 routing isolation tests |
| `tests/test_reconnect.py` | Create | 2 disconnect/reconnect tests |
| `tests/test_errors.py` | Create | 3 error-path tests |

---

## Task 1: Project config — test deps + pytest.ini

**Files:**
- Modify: `pyproject.toml`
- Create: `pytest.ini`
- Create: `tests/__init__.py`

- [ ] **Step 1: Add test dependencies to `pyproject.toml`**

Add after the existing `[project]` section:

```toml
[project.optional-dependencies]
test = ["pytest>=8", "pytest-asyncio>=0.24", "pytest-timeout"]
```

- [ ] **Step 2: Create `pytest.ini`**

```ini
[pytest]
asyncio_mode = auto
timeout = 120
```

- [ ] **Step 3: Create `tests/__init__.py`**

Empty file:
```python
```

- [ ] **Step 4: Install test deps**

```bash
pip install -e ".[test]"
```

Expected: installs pytest, pytest-asyncio, pytest-timeout without errors.

- [ ] **Step 5: Verify pytest discovers tests directory**

```bash
pytest tests/ --collect-only
```

Expected: `no tests ran` (no test files yet), exit 0.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml pytest.ini tests/__init__.py
git commit -m "test: add pytest config and test deps"
```

---

## Task 2: `tests/helpers.py` — polling and WS utilities

**Files:**
- Create: `tests/helpers.py`

- [ ] **Step 1: Create `tests/helpers.py`**

```python
"""Shared helpers for JRK integration tests."""

import asyncio
import json
import time
import uuid

import aiohttp


BASE_URL = "http://localhost:18890"
TOKEN = "test"
AUTH = {"Authorization": f"token {TOKEN}"}
JRK = f"{BASE_URL}/jrk"


def wait_for_http(url: str, headers: dict, timeout: int = 60, interval: float = 1.0) -> None:
    """Block until GET url returns 200, or raise TimeoutError."""
    import urllib.request, urllib.error
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {url}")


def poll_tunnel_registered(agent_name: str, timeout: int = 30) -> None:
    """Block until agent_name appears in /jrk/debug/tunnels."""
    import urllib.request
    url = f"{JRK}/debug/tunnels"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, headers=AUTH)
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read())
                if agent_name in data.get("tunnels", {}):
                    return
        except Exception:
            pass
        time.sleep(1.0)
    raise TimeoutError(f"Agent {agent_name!r} did not register within {timeout}s")


async def poll_kernel_idle(kernel_id: str, timeout: int = 30) -> None:
    """Wait until kernel execution_state == 'idle'. Restarts once if needed."""
    url = f"{JRK}/api/kernels/{kernel_id}"
    restart_url = f"{JRK}/api/kernels/{kernel_id}/restart"
    deadline = asyncio.get_event_loop().time() + timeout
    restarted = False

    async with aiohttp.ClientSession(headers=AUTH) as session:
        while asyncio.get_event_loop().time() < deadline:
            async with session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("execution_state") == "idle":
                        return
            await asyncio.sleep(1.0)

            # One restart attempt at halfway point
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining < timeout / 2 and not restarted:
                restarted = True
                async with session.post(restart_url) as _:
                    pass

    raise TimeoutError(f"Kernel {kernel_id} did not reach idle within {timeout}s")


async def ws_execute(kernel_id: str, code: str, timeout: int = 30) -> dict:
    """
    Open WS to /jrk/api/kernels/{id}/channels, send execute_request,
    collect stream/execute_result/execute_reply, return result dict.

    Returns:
        {"output": str, "status": "ok"|"error", "execution_count": int,
         "ename": str|None, "evalue": str|None}
    """
    ws_url = f"ws://localhost:18890/jrk/api/kernels/{kernel_id}/channels"
    msg_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    execute_request = {
        "header": {
            "msg_id": msg_id,
            "msg_type": "execute_request",
            "session": session_id,
            "username": "test",
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
        },
        "channel": "shell",
    }

    output_parts = []
    status = None
    execution_count = None
    ename = None
    evalue = None

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url,
            headers=AUTH,
            timeout=aiohttp.ClientWSTimeout(ws_connect=10),
        ) as ws:
            await ws.send_str(json.dumps(execute_request))

            deadline = asyncio.get_event_loop().time() + timeout
            async for msg in ws:
                if asyncio.get_event_loop().time() > deadline:
                    raise TimeoutError(f"ws_execute timed out waiting for reply")
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                if data.get("parent_header", {}).get("msg_id") != msg_id:
                    continue  # not our reply

                mt = data.get("msg_type") or data.get("header", {}).get("msg_type", "")
                if mt == "stream":
                    output_parts.append(data["content"]["text"])
                elif mt == "execute_result":
                    output_parts.append(data["content"]["data"].get("text/plain", ""))
                    execution_count = data["content"].get("execution_count")
                elif mt == "error":
                    ename = data["content"].get("ename")
                    evalue = data["content"].get("evalue")
                elif mt == "execute_reply":
                    status = data["content"]["status"]
                    if execution_count is None:
                        execution_count = data["content"].get("execution_count")
                    break  # done

    return {
        "output": "".join(output_parts),
        "status": status,
        "execution_count": execution_count,
        "ename": ename,
        "evalue": evalue,
    }
```

- [ ] **Step 2: Verify imports work**

```bash
python3 -c "from tests.helpers import ws_execute, poll_kernel_idle; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add tests/helpers.py
git commit -m "test: add test helper utilities (polling, ws_execute)"
```

---

## Task 3: `tests/conftest.py` — subprocess fixtures

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `tests/conftest.py`**

```python
"""pytest fixtures: start/stop JupyterLab + agents as subprocesses."""

import subprocess
import sys
import time

import pytest

from tests.helpers import (
    AUTH, JRK, TOKEN, BASE_URL,
    wait_for_http, poll_tunnel_registered,
    poll_kernel_idle,
)
import aiohttp


@pytest.fixture(scope="session")
def jupyterlab():
    """Start JupyterLab with JRK extension on port 18890."""
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "jupyterlab",
            "--no-browser", "--ip=127.0.0.1", "--port=18890",
            f"--ServerApp.token={TOKEN}",
            "--GatewayClient.url=http://localhost:18890/jrk",
            f"--GatewayClient.auth_token={TOKEN}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_http(f"{JRK}/api/kernelspecs", headers=AUTH, timeout=60)
        yield {"base_url": BASE_URL, "token": TOKEN}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _start_agent(name: str) -> subprocess.Popen:
    return subprocess.Popen(
        [
            "jupyter-remote-kernel", "agent",
            "--hub", "http://localhost:18890/jrk",
            "--name", name,
            "--token", TOKEN,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


@pytest.fixture(scope="session")
def agent1(jupyterlab):
    proc = _start_agent("agent1")
    try:
        poll_tunnel_registered("agent1", timeout=30)
        yield "agent1"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def agent2(jupyterlab):
    proc = _start_agent("agent2")
    try:
        poll_tunnel_registered("agent2", timeout=30)
        yield "agent2"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
async def kernel(agent1):
    """Function-scoped fixture: create agent1:python3 kernel, delete on teardown."""
    async with aiohttp.ClientSession(headers=AUTH) as session:
        async with session.post(
            f"{JRK}/api/kernels",
            json={"name": "agent1:python3"},
        ) as r:
            assert r.status == 200, f"kernel create failed: {r.status}"
            data = await r.json()
            kid = data["id"]

    await poll_kernel_idle(kid, timeout=30)

    yield kid

    async with aiohttp.ClientSession(headers=AUTH) as session:
        async with session.delete(f"{JRK}/api/kernels/{kid}") as r:
            pass  # ignore teardown errors
```

- [ ] **Step 2: Verify conftest loads without error**

```bash
pytest tests/ --collect-only 2>&1 | head -20
```

Expected: no import errors; exits 0 or "no tests" warning.

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "test: add session-scoped subprocess fixtures for JupyterLab and agents"
```

---

## Task 4: `test_kernelspecs.py`

**Files:**
- Create: `tests/test_kernelspecs.py`

- [ ] **Step 1: Create `tests/test_kernelspecs.py`**

```python
"""Tests for GET /jrk/api/kernelspecs."""

import aiohttp
import pytest

from tests.helpers import AUTH, JRK


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, agent2):
    """Ensure both agents are registered before any test in this module."""
    pass


async def test_list_kernelspecs():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernelspecs") as r:
            assert r.status == 200
            data = await r.json()
    kernels = data["kernelspecs"]
    assert "agent1:python3" in kernels
    assert "agent2:python3" in kernels


async def test_kernelspec_name_field():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernelspecs") as r:
            data = await r.json()
    for key, spec in data["kernelspecs"].items():
        assert spec["name"] == key, (
            f"kernelspec {key!r} has name={spec['name']!r}; "
            "must match key for correct kernel creation routing"
        )


async def test_default_is_empty_string():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernelspecs") as r:
            data = await r.json()
    assert data["default"] == "", (
        f"default={data['default']!r}; JupyterLab requires empty string, not null"
    )


async def test_get_single_kernelspec():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernelspecs/agent1:python3") as r:
            assert r.status == 200
            data = await r.json()
    assert data["name"] == "agent1:python3"
```

- [ ] **Step 2: Run kernelspec tests (agents must be running)**

```bash
pytest tests/test_kernelspecs.py -v
```

Expected: 4 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_kernelspecs.py
git commit -m "test: add kernelspec endpoint tests"
```

---

## Task 5: `test_kernels.py`

**Files:**
- Create: `tests/test_kernels.py`

- [ ] **Step 1: Create `tests/test_kernels.py`**

```python
"""Tests for kernel CRUD endpoints."""

import aiohttp
import pytest

from tests.helpers import AUTH, JRK, poll_kernel_idle


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, agent2):
    pass


async def test_create_kernel():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent1:python3"}) as r:
            assert r.status == 200
            data = await r.json()
    assert "id" in data
    assert data["name"] == "agent1:python3"

    # cleanup
    async with aiohttp.ClientSession(headers=AUTH) as s:
        await s.delete(f"{JRK}/api/kernels/{data['id']}")


async def test_get_kernel(kernel):
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernels/{kernel}") as r:
            assert r.status == 200
            data = await r.json()
    assert data["id"] == kernel
    assert "name" in data
    assert "execution_state" in data


async def test_list_kernels(kernel):
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernels") as r:
            assert r.status == 200
            data = await r.json()
    ids = [k["id"] for k in data]
    assert kernel in ids


async def test_delete_kernel(agent1):
    # Create a fresh kernel specifically to delete
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent1:python3"}) as r:
            kid = (await r.json())["id"]

    await poll_kernel_idle(kid, timeout=30)

    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.delete(f"{JRK}/api/kernels/{kid}") as r:
            assert r.status == 204

        async with s.get(f"{JRK}/api/kernels/{kid}") as r:
            assert r.status == 404


async def test_restart_kernel(kernel):
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels/{kernel}/restart") as r:
            assert r.status == 200

    await poll_kernel_idle(kernel, timeout=30)


async def test_interrupt_kernel(kernel):
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels/{kernel}/interrupt") as r:
            assert r.status == 204
```

- [ ] **Step 2: Run kernel CRUD tests**

```bash
pytest tests/test_kernels.py -v
```

Expected: 6 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_kernels.py
git commit -m "test: add kernel CRUD endpoint tests"
```

---

## Task 6: `test_execution.py`

**Files:**
- Create: `tests/test_execution.py`

- [ ] **Step 1: Create `tests/test_execution.py`**

```python
"""Tests for kernel WebSocket code execution."""

import pytest

from tests.helpers import ws_execute


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, agent2):
    pass


async def test_execute_print(kernel):
    result = await ws_execute(kernel, 'print("hello")')
    assert result["status"] == "ok"
    assert result["output"] == "hello\n"


async def test_execute_expression(kernel):
    result = await ws_execute(kernel, "1 + 1")
    assert result["status"] == "ok"
    assert result["output"].strip() == "2"


async def test_execute_error(kernel):
    result = await ws_execute(kernel, 'raise ValueError("boom")')
    assert result["status"] == "error"
    assert result["ename"] == "ValueError"
    assert "boom" in result["evalue"]
```

- [ ] **Step 2: Run execution tests**

```bash
pytest tests/test_execution.py -v
```

Expected: 3 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_execution.py
git commit -m "test: add kernel WS execution tests"
```

---

## Task 7: `test_routing.py`

**Files:**
- Create: `tests/test_routing.py`

- [ ] **Step 1: Create `tests/test_routing.py`**

```python
"""Tests that kernels route to the correct agent."""

import aiohttp
import pytest

from tests.helpers import AUTH, JRK, poll_kernel_idle, ws_execute


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, agent2):
    pass


async def test_kernels_route_to_different_agents():
    """Kernels on agent1 and agent2 run in different ipykernel processes."""
    kid1 = kid2 = None
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent1:python3"}) as r:
            kid1 = (await r.json())["id"]
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent2:python3"}) as r:
            kid2 = (await r.json())["id"]

    try:
        await poll_kernel_idle(kid1, timeout=30)
        await poll_kernel_idle(kid2, timeout=30)

        r1 = await ws_execute(kid1, "import os; print(os.getpid())")
        r2 = await ws_execute(kid2, "import os; print(os.getpid())")

        assert r1["status"] == "ok"
        assert r2["status"] == "ok"
        pid1 = int(r1["output"].strip())
        pid2 = int(r2["output"].strip())
        assert pid1 != pid2, (
            f"Both kernels returned PID {pid1}; expected different processes"
        )
    finally:
        async with aiohttp.ClientSession(headers=AUTH) as s:
            if kid1:
                await s.delete(f"{JRK}/api/kernels/{kid1}")
            if kid2:
                await s.delete(f"{JRK}/api/kernels/{kid2}")


async def test_kernelspec_prefix_routing():
    """A kernel named 'agent2:python3' is routed to agent2, not agent1."""
    kid = None
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent2:python3"}) as r:
            assert r.status == 200
            data = await r.json()
            kid = data["id"]

    try:
        await poll_kernel_idle(kid, timeout=30)

        # Verify the kernel is actually running on agent2's Jupyter Server
        # by checking the hub's kernel_tunnel mapping via debug endpoint
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.get(f"{JRK}/debug/tunnels") as r:
                tunnels = await r.json()

        kernel_tunnels = tunnels.get("kernel_tunnel", {})
        assert kid in kernel_tunnels, f"kernel {kid} not in kernel_tunnel mapping"
        assert kernel_tunnels[kid] == "agent2", (
            f"kernel {kid} mapped to {kernel_tunnels[kid]!r}, expected 'agent2'"
        )
    finally:
        async with aiohttp.ClientSession(headers=AUTH) as s:
            if kid:
                await s.delete(f"{JRK}/api/kernels/{kid}")
```

- [ ] **Step 2: Run routing tests**

```bash
pytest tests/test_routing.py -v
```

Expected: 2 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_routing.py
git commit -m "test: add kernel routing isolation tests"
```

---

## Task 8: `test_reconnect.py`

**Files:**
- Create: `tests/test_reconnect.py`

Note: These tests use **function-scoped** agent fixtures (not the session-scoped ones) so they can kill/restart the agent without affecting other tests.

- [ ] **Step 1: Create `tests/test_reconnect.py`**

```python
"""Tests for agent disconnect/reconnect behaviour."""

import subprocess
import sys
import time

import aiohttp
import pytest

from tests.helpers import (
    AUTH, JRK, TOKEN,
    poll_tunnel_registered, poll_kernel_idle,
)
from tests.conftest import _start_agent


@pytest.fixture(scope="module", autouse=True)
def require_jupyterlab(jupyterlab):
    pass


@pytest.fixture
def restartable_agent():
    """Function-scoped agent that tests can kill and restart."""
    proc = _start_agent("agent-restart-test")
    poll_tunnel_registered("agent-restart-test", timeout=30)
    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


async def test_agent_disconnect_cleans_state(restartable_agent):
    """After agent disconnects, hub removes it from tunnel registry."""
    restartable_agent.terminate()
    restartable_agent.wait(timeout=10)

    # Give hub up to 5s to process the disconnect
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.get(f"{JRK}/debug/tunnels") as r:
                data = await r.json()
        if "agent-restart-test" not in data.get("tunnels", {}):
            return
        time.sleep(0.5)

    raise AssertionError("agent-restart-test still in tunnels after disconnect")


async def test_agent_reconnect_restores_kernels(jupyterlab):
    """After agent reconnects, kernels created before disconnect are restored."""
    proc = _start_agent("agent-restore-test")
    try:
        poll_tunnel_registered("agent-restore-test", timeout=30)

        # Create a kernel
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.post(
                f"{JRK}/api/kernels", json={"name": "agent-restore-test:python3"}
            ) as r:
                assert r.status == 200
                kid = (await r.json())["id"]

        await poll_kernel_idle(kid, timeout=30)

        # Kill the agent
        proc.terminate()
        proc.wait(timeout=10)

        # Wait for hub to clear the tunnel
        time.sleep(2)

        # Restart agent
        proc2 = _start_agent("agent-restore-test")
        try:
            poll_tunnel_registered("agent-restore-test", timeout=30)

            # Kernel should be restored in hub's kernel_tunnel
            async with aiohttp.ClientSession(headers=AUTH) as s:
                async with s.get(f"{JRK}/api/kernels/{kid}") as r:
                    assert r.status == 200, (
                        f"Kernel {kid} not found after agent reconnect"
                    )
        finally:
            proc2.terminate()
            try:
                proc2.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc2.kill()

        # Cleanup kernel
        async with aiohttp.ClientSession(headers=AUTH) as s:
            await s.delete(f"{JRK}/api/kernels/{kid}")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
```

- [ ] **Step 2: Run reconnect tests**

```bash
pytest tests/test_reconnect.py -v
```

Expected: 2 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_reconnect.py
git commit -m "test: add agent disconnect/reconnect tests"
```

---

## Task 9: `test_errors.py`

**Files:**
- Create: `tests/test_errors.py`

- [ ] **Step 1: Create `tests/test_errors.py`**

```python
"""Tests for error paths."""

import subprocess
import sys
import time

import aiohttp
import pytest

from tests.helpers import AUTH, JRK, TOKEN
from tests.conftest import _start_agent


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, jupyterlab):
    pass


async def test_duplicate_agent_name_rejected():
    """A second agent with the same name must be rejected and exit non-zero."""
    proc = _start_agent("agent1")  # agent1 is already registered (session fixture)
    try:
        # Give it up to 10s to exit
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            ret = proc.poll()
            if ret is not None:
                assert ret != 0, (
                    f"Duplicate agent exited 0 (success); expected non-zero rejection"
                )
                return
            time.sleep(0.5)
        raise AssertionError("Duplicate agent did not exit within 10s")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass


async def test_get_nonexistent_kernel():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernels/does-not-exist") as r:
            assert r.status == 404


async def test_delete_nonexistent_kernel():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.delete(f"{JRK}/api/kernels/does-not-exist") as r:
            assert r.status == 404
```

- [ ] **Step 2: Run error tests**

```bash
pytest tests/test_errors.py -v
```

Expected: 3 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_errors.py
git commit -m "test: add error path tests (duplicate name, 404s)"
```

---

## Task 10: Full suite run + final commit

- [ ] **Step 1: Run full suite**

```bash
pytest tests/ -v
```

Expected: all 20 tests PASSED. Total time will be 60-120s due to JupyterLab startup.

- [ ] **Step 2: Add `pytest-timeout` to guard against hangs**

Already added in Task 1. Verify `pytest.ini` has `timeout = 120`.

- [ ] **Step 3: Final commit**

```bash
git add .
git commit -m "test: complete integration test suite (20 tests, extension mode)"
```

---

## Self-Review Notes

- **Spec coverage**: All 6 test files and 20 tests from the spec are implemented.
- **`debug/tunnels` response shape**: `test_routing.py` assumes `kernel_tunnel` is a flat `{kernel_id: agent_name}` dict — must verify against actual endpoint response in `server_extension.py`. Adjust key path if the shape differs.
- **`_start_agent` exported from `conftest.py`**: `test_reconnect.py` imports `_start_agent` from `conftest`. This is a private helper; if this causes import issues, move it to `helpers.py`.
- **`test_reconnect.py` is not order-dependent**: It uses agent names `agent-restart-test` and `agent-restore-test`, different from the session fixtures `agent1`/`agent2`.
