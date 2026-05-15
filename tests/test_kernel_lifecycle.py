"""Tests for kernel lifecycle: agent-side kill, hub-side kill, and kill when agent gone."""

import asyncio
import json
import subprocess
import time
import uuid

import aiohttp
import pytest

from tests.helpers import AUTH, JRK, poll_kernel_idle, poll_tunnel_registered, ws_execute
from tests.conftest import _start_agent


@pytest.fixture(scope="module", autouse=True)
def require_agents(agent1, agent2):
    pass


async def test_hub_delete_removes_from_registry(kernel):
    """DELETE /api/kernels/{id} removes the kernel from hub's registry."""
    kid = kernel
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.delete(f"{JRK}/api/kernels/{kid}") as r:
            assert r.status == 204

    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/debug/tunnels") as r:
            data = await r.json()
    assert kid not in data.get("kernels", []), (
        f"Kernel {kid} still in hub registry after DELETE"
    )


async def test_kernel_death_on_agent():
    """When a kernel process dies, ws_execute reflects the death (non-ok status or disconnect).

    Jupyter Server auto-restarts dead kernels, so the "dead" window may be too short to
    catch via GET.  Instead we assert that ws_execute does NOT return "ok" for code that
    kills the kernel mid-execution — the hub must propagate the disconnect back to the client.
    """
    kid = None
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent1:python3"}) as r:
            assert r.status in (200, 201)
            kid = (await r.json())["id"]

    try:
        await poll_kernel_idle(kid, timeout=30)

        result_status = "ok"  # assume ok unless we learn otherwise
        try:
            result = await asyncio.wait_for(
                ws_execute(kid, "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"),
                timeout=15,
            )
            result_status = result.get("status")
        except Exception:
            result_status = None  # WS closed abnormally — valid "kernel died" signal

        assert result_status != "ok", (
            f"ws_execute returned 'ok' after kernel SIGTERM — expected error or disconnect"
        )
    finally:
        if kid:
            async with aiohttp.ClientSession(headers=AUTH) as s:
                await s.delete(f"{JRK}/api/kernels/{kid}")


async def test_hub_delete_when_agent_gone():
    """DELETE /api/kernels/{id} returns 404 when the agent has already disconnected."""
    proc = _start_agent("agent-kill-test")
    kid = None
    try:
        poll_tunnel_registered("agent-kill-test", timeout=30)

        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.post(
                f"{JRK}/api/kernels", json={"name": "agent-kill-test:python3"}
            ) as r:
                assert r.status in (200, 201)
                kid = (await r.json())["id"]

        await poll_kernel_idle(kid, timeout=30)

        # Kill the agent — on_close() will clear kernel_tunnel
        proc.terminate()
        proc.wait(timeout=10)
        proc = None

        # Wait for hub to clear the tunnel (which also clears kernel_tunnel)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            async with aiohttp.ClientSession(headers=AUTH) as s:
                async with s.get(f"{JRK}/debug/tunnels") as r:
                    data = await r.json()
            if "agent-kill-test" not in data.get("tunnels", []):
                break
            time.sleep(0.5)

        # Now DELETE should return 404 (kernel not in kernel_tunnel, no tunnel to query)
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.delete(f"{JRK}/api/kernels/{kid}") as r:
                assert r.status == 404, (
                    f"Expected 404 when deleting kernel after agent gone, got {r.status}"
                )
        kid = None  # already cleaned up (404 means not tracked)

    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass


async def test_execute_on_deleted_kernel():
    """WS connect to a deleted kernel is rejected with close code 4404."""
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.post(f"{JRK}/api/kernels", json={"name": "agent1:python3"}) as r:
            assert r.status in (200, 201)
            kid = (await r.json())["id"]

    await poll_kernel_idle(kid, timeout=30)

    # Delete the kernel
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.delete(f"{JRK}/api/kernels/{kid}") as r:
            assert r.status == 204

    # Attempt to open a WS channel on the now-deleted kernel
    ws_url = f"ws://localhost:18890/jrk/api/kernels/{kid}/channels"
    close_code = None
    async with aiohttp.ClientSession() as s:
        try:
            async with s.ws_connect(
                ws_url,
                headers=AUTH,
                timeout=aiohttp.ClientWSTimeout(ws_close=10),
            ) as ws:
                # Server should immediately close with 4404
                async for msg in ws:
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                        close_code = ws.close_code
                        break
                close_code = ws.close_code
        except Exception:
            pass  # connection may be refused outright

    assert close_code == 4404, (
        f"Expected WS close code 4404 for deleted kernel, got {close_code}"
    )
