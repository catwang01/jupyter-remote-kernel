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
    # default must be a string (not null); either empty or a valid kernelspec name
    assert isinstance(data["default"], str), (
        f"default={data['default']!r}; JupyterLab requires a string, not null"
    )


async def test_get_single_kernelspec():
    async with aiohttp.ClientSession(headers=AUTH) as s:
        async with s.get(f"{JRK}/api/kernelspecs/agent1:python3") as r:
            assert r.status == 200
            data = await r.json()
    assert data["name"] == "agent1:python3"
