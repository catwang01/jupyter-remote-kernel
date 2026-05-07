"""
Remote agent: starts a local Jupyter Server, then connects to the Hub via
an outbound WebSocket (works through NAT / firewalls).  The Hub uses this
persistent connection to relay HTTP requests and kernel WebSocket frames.

Usage:
    jupyter-remote-kernel agent --hub http://hub-ip:8765 --name gpu-machine
"""

import asyncio
import base64
import json
import os
import secrets
import socket
import subprocess
import sys
from typing import Dict, Optional

import aiohttp
from aiohttp import WSMsgType


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RemoteAgent:
    def __init__(self, hub_url: str, name: str, jupyter_port: int = 0, root_dir: str = "", hub_token: str = ""):
        base = hub_url.rstrip("/")
        ws_base = base.replace("https://", "wss://").replace("http://", "ws://")
        self.tunnel_url = f"{ws_base}/tunnel/register"
        self.name = name
        self.hub_token = hub_token
        self.jupyter_port = jupyter_port or _free_port()
        self.root_dir = root_dir
        self.jupyter_base_http = f"http://localhost:{self.jupyter_port}"
        self.jupyter_base_ws = f"ws://localhost:{self.jupyter_port}"
        self.token = secrets.token_hex(16)
        self._local_ws: Dict[str, aiohttp.ClientWebSocketResponse] = {}

    # ── Jupyter Server lifecycle ─────────────────────────────────────────────

    async def _start_jupyter(self) -> subprocess.Popen:
        # Kill any existing process on this port to avoid token mismatch on restart
        subprocess.run(
            f"lsof -ti tcp:{self.jupyter_port} 2>/dev/null | xargs kill -9 2>/dev/null || "
            f"fuser -k {self.jupyter_port}/tcp 2>/dev/null || true",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        await asyncio.sleep(1)

        cmd = [
            sys.executable, "-m", "jupyter_server",
            "--no-browser",
            f"--port={self.jupyter_port}",
            f"--ServerApp.token={self.token}",
            "--ServerApp.ip=127.0.0.1",
            "--ServerApp.allow_remote_access=False",
        ]
        cwd = None
        if self.root_dir:
            os.makedirs(self.root_dir, exist_ok=True)
            cmd.append(f"--ServerApp.root_dir={self.root_dir}")
            cwd = self.root_dir
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"[Agent] Starting Jupyter Server on port {self.jupyter_port} ...")
        async with aiohttp.ClientSession() as s:
            for _ in range(30):
                await asyncio.sleep(1)
                try:
                    async with s.get(
                        f"{self.jupyter_base_http}/api",
                        headers=self._auth(),
                    ) as r:
                        if r.status == 200:
                            print(f"[Agent] Jupyter ready")
                            return proc
                except Exception:
                    pass
        proc.kill()
        raise RuntimeError("Jupyter Server did not start within 30 s")

    def _auth(self) -> dict:
        return {"Authorization": f"token {self.token}"}

    # ── Relay: HTTP ──────────────────────────────────────────────────────────

    async def _relay_http(
        self,
        session: aiohttp.ClientSession,
        hub_ws: aiohttp.ClientWebSocketResponse,
        msg: dict,
    ) -> None:
        req_id = msg["req_id"]
        url = f"{self.jupyter_base_http}{msg['path']}"
        headers = dict(msg.get("headers") or {})
        headers.update(self._auth())
        try:
            async with session.request(
                msg["method"], url, headers=headers, data=msg.get("body")
            ) as resp:
                body = await resp.text()
                await hub_ws.send_json({
                    "type": "http_res",
                    "req_id": req_id,
                    "status": resp.status,
                    "headers": dict(resp.headers),
                    "body": body,
                })
        except Exception as e:
            await hub_ws.send_json({
                "type": "http_res",
                "req_id": req_id,
                "status": 500,
                "headers": {},
                "body": json.dumps({"error": str(e)}),
            })

    # ── Relay: WebSocket ─────────────────────────────────────────────────────

    async def _relay_ws_to_hub(
        self,
        hub_ws: aiohttp.ClientWebSocketResponse,
        local_ws: aiohttp.ClientWebSocketResponse,
        ws_id: str,
    ) -> None:
        """Forward frames from local Jupyter → Hub (runs until WS closes)."""
        try:
            async for msg in local_ws:
                if msg.type == WSMsgType.TEXT:
                    await hub_ws.send_json({
                        "type": "ws_frame", "ws_id": ws_id,
                        "data": msg.data, "binary": False,
                    })
                elif msg.type == WSMsgType.BINARY:
                    await hub_ws.send_json({
                        "type": "ws_frame", "ws_id": ws_id,
                        "data": base64.b64encode(msg.data).decode(), "binary": True,
                    })
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
        finally:
            self._local_ws.pop(ws_id, None)
            try:
                await hub_ws.send_json({"type": "ws_close", "ws_id": ws_id})
            except Exception:
                pass

    async def _open_ws(
        self,
        session: aiohttp.ClientSession,
        hub_ws: aiohttp.ClientWebSocketResponse,
        msg: dict,
    ) -> None:
        ws_id = msg["ws_id"]
        url = f"{self.jupyter_base_ws}{msg['path']}"
        try:
            local_ws = await session.ws_connect(url, headers=self._auth())
            self._local_ws[ws_id] = local_ws
            await hub_ws.send_json({"type": "ws_opened", "ws_id": ws_id})
            # Fire-and-forget relay task; closes itself when the WS ends
            asyncio.create_task(self._relay_ws_to_hub(hub_ws, local_ws, ws_id))
        except Exception as e:
            await hub_ws.send_json({
                "type": "ws_reject", "ws_id": ws_id, "error": str(e),
            })

    # ── Main loop ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        proc = await self._start_jupyter()
        print(f"[Agent] Connecting to Hub: {self.tunnel_url}")

        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    try:
                        headers = {}
                        if self.hub_token:
                            headers["Authorization"] = f"token {self.hub_token}"
                        async with session.ws_connect(self.tunnel_url, headers=headers) as ws:
                            await ws.send_json({"type": "register", "name": self.name, "token": self.hub_token})

                            async for msg in ws:
                                if msg.type == WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    t = data.get("type")

                                    if t == "registered":
                                        print(f"[Agent] Registered as '{self.name}' — ready")

                                    elif t == "http_req":
                                        asyncio.create_task(
                                            self._relay_http(session, ws, data)
                                        )

                                    elif t == "ws_open":
                                        asyncio.create_task(
                                            self._open_ws(session, ws, data)
                                        )

                                    elif t == "ws_frame":
                                        local = self._local_ws.get(data["ws_id"])
                                        if local:
                                            if data["binary"]:
                                                await local.send_bytes(
                                                    base64.b64decode(data["data"])
                                                )
                                            else:
                                                await local.send_str(data["data"])

                                    elif t == "ws_close":
                                        local = self._local_ws.pop(data["ws_id"], None)
                                        if local:
                                            await local.close()

                                elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                    break

                    except (KeyboardInterrupt, asyncio.CancelledError):
                        raise
                    except Exception as e:
                        print(f"[Agent] Lost connection: {e}  — retrying in 5 s ...")
                        await asyncio.sleep(5)
        finally:
            proc.kill()
