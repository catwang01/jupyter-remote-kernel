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
            assert r.status in (200, 201), f"kernel create failed: {r.status}"
            data = await r.json()
            kid = data["id"]

    await poll_kernel_idle(kid, timeout=30)

    yield kid

    async with aiohttp.ClientSession(headers=AUTH) as session:
        async with session.delete(f"{JRK}/api/kernels/{kid}") as r:
            pass  # ignore teardown errors
