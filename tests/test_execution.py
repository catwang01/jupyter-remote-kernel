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
