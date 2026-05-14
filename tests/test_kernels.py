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
            assert r.status in (200, 201)
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
