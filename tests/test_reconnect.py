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
        if "agent-restart-test" not in data.get("tunnels", []):
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
