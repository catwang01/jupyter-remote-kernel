"""Tests for error paths."""

import subprocess
import time

import aiohttp
import pytest

from tests.helpers import AUTH, JRK, TOKEN
from tests.conftest import _start_agent


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, jupyterlab):
    pass


async def test_duplicate_agent_name_rejected():
    """A second agent with the same name must not be added to the tunnel registry."""
    proc = _start_agent("agent1")  # agent1 is already registered (session fixture)
    try:
        # Wait for the duplicate agent's registration attempt to complete
        time.sleep(3)

        # The tunnel registry must still have exactly the original agent1
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.get(f"{JRK}/debug/tunnels") as r:
                data = await r.json()
        tunnels = data.get("tunnels", [])
        assert "agent1" in tunnels, "agent1 tunnel disappeared after duplicate attempt"
        # There must not be two separate entries for agent1 (list has unique names)
        assert tunnels.count("agent1") == 1, (
            f"Expected exactly 1 'agent1' tunnel entry, got {tunnels.count('agent1')}"
        )
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
