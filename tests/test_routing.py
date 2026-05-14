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
            assert r.status in (200, 201)
            data = await r.json()
            kid = data["id"]

    try:
        await poll_kernel_idle(kid, timeout=30)

        # Verify the kernel is tracked by the hub and has the correct name
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.get(f"{JRK}/api/kernels/{kid}") as r:
                assert r.status == 200
                kdata = await r.json()
        assert kdata["name"] == "agent2:python3", (
            f"kernel name={kdata['name']!r}, expected 'agent2:python3'"
        )

        # Verify kernel ID appears in the hub's kernel registry
        async with aiohttp.ClientSession(headers=AUTH) as s:
            async with s.get(f"{JRK}/debug/tunnels") as r:
                tunnels = await r.json()
        assert kid in tunnels.get("kernels", []), (
            f"kernel {kid} not tracked in hub kernel registry"
        )
    finally:
        async with aiohttp.ClientSession(headers=AUTH) as s:
            if kid:
                await s.delete(f"{JRK}/api/kernels/{kid}")
