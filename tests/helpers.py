"""Shared helpers for JRK integration tests."""

import asyncio
import json
import time
import uuid

import aiohttp


BASE_URL = "http://localhost:18890"
TOKEN = "test"
AUTH = {"Authorization": f"token {TOKEN}"}
JRK = f"{BASE_URL}/jrk"


def wait_for_http(url: str, headers: dict, timeout: int = 60, interval: float = 1.0) -> None:
    """Block until GET url returns 200, or raise TimeoutError."""
    import urllib.request, urllib.error
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {url}")


def poll_tunnel_registered(agent_name: str, timeout: int = 30) -> None:
    """Block until agent_name appears in /jrk/debug/tunnels."""
    import urllib.request
    url = f"{JRK}/debug/tunnels"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, headers=AUTH)
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read())
                if agent_name in data.get("tunnels", {}):
                    return
        except Exception:
            pass
        time.sleep(1.0)
    raise TimeoutError(f"Agent {agent_name!r} did not register within {timeout}s")


async def poll_kernel_idle(kernel_id: str, timeout: int = 30) -> None:
    """Wait until kernel execution_state == 'idle'. Restarts once if needed."""
    url = f"{JRK}/api/kernels/{kernel_id}"
    restart_url = f"{JRK}/api/kernels/{kernel_id}/restart"
    deadline = asyncio.get_event_loop().time() + timeout
    restarted = False

    async with aiohttp.ClientSession(headers=AUTH) as session:
        while asyncio.get_event_loop().time() < deadline:
            async with session.get(url) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("execution_state") == "idle":
                        return
            await asyncio.sleep(1.0)

            # One restart attempt at halfway point
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining < timeout / 2 and not restarted:
                restarted = True
                async with session.post(restart_url) as _:
                    pass

    raise TimeoutError(f"Kernel {kernel_id} did not reach idle within {timeout}s")


async def ws_execute(kernel_id: str, code: str, timeout: int = 30) -> dict:
    """
    Open WS to /jrk/api/kernels/{id}/channels, send execute_request,
    collect stream/execute_result/execute_reply, return result dict.

    Returns:
        {"output": str, "status": "ok"|"error", "execution_count": int,
         "ename": str|None, "evalue": str|None}
    """
    ws_url = f"ws://localhost:18890/jrk/api/kernels/{kernel_id}/channels"
    msg_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    execute_request = {
        "header": {
            "msg_id": msg_id,
            "msg_type": "execute_request",
            "session": session_id,
            "username": "test",
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
        },
        "channel": "shell",
    }

    output_parts = []
    status = None
    execution_count = None
    ename = None
    evalue = None

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url,
            headers=AUTH,
            timeout=aiohttp.ClientWSTimeout(ws_close=10),
        ) as ws:
            await ws.send_str(json.dumps(execute_request))

            deadline = asyncio.get_event_loop().time() + timeout
            async for msg in ws:
                if asyncio.get_event_loop().time() > deadline:
                    raise TimeoutError(f"ws_execute timed out waiting for reply")
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                if data.get("parent_header", {}).get("msg_id") != msg_id:
                    continue  # not our reply

                mt = data.get("msg_type") or data.get("header", {}).get("msg_type", "")
                if mt == "stream":
                    output_parts.append(data["content"]["text"])
                elif mt == "execute_result":
                    output_parts.append(data["content"]["data"].get("text/plain", ""))
                    execution_count = data["content"].get("execution_count")
                elif mt == "error":
                    ename = data["content"].get("ename")
                    evalue = data["content"].get("evalue")
                elif mt == "execute_reply":
                    status = data["content"]["status"]
                    if execution_count is None:
                        execution_count = data["content"].get("execution_count")
                    break  # done

    return {
        "output": "".join(output_parts),
        "status": status,
        "execution_count": execution_count,
        "ename": ename,
        "evalue": evalue,
    }
