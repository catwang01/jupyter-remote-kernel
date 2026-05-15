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


async def test_execute_long_running(kernel_via_jlab):
    # Regression: WS relay must stay alive when kernel produces no output for
    # several seconds.  Tests via JupyterLab's native /api/kernels/{id}/channels
    # (same path as MCP tools / browser), which proxies through GatewayClient to
    # JRK — not the direct /jrk/ endpoint used by the other tests above.
    result = await ws_execute(
        kernel_via_jlab, 'import time; time.sleep(5); print("alive")',
        timeout=30, via_jlab=True,
    )
    assert result["status"] == "ok"
    assert "alive" in result["output"]
