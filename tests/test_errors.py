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
