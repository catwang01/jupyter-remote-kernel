"""
Hub server: accepts WebSocket tunnels from remote agents, exposes Jupyter
Gateway-compatible API so JupyterLab can start kernels on remote machines
without direct inbound connectivity to those machines.

JupyterLab config:
    jupyter lab --GatewayClient.url=http://hub-ip:8765
"""

import asyncio
import base64
import json
import uuid
from typing import Dict, Optional

from aiohttp import web, WSMsgType


# ── Tunnel connection ────────────────────────────────────────────────────────

class TunnelConnection:
    """Wraps the persistent WebSocket to one remote agent.

    Protocol messages (all JSON):
      Hub → Remote:
        {type: http_req,  req_id, method, path, headers, body}
        {type: ws_open,   ws_id, path}
        {type: ws_frame,  ws_id, data, binary}
        {type: ws_close,  ws_id}

      Remote → Hub:
        {type: http_res,  req_id, status, headers, body}
        {type: ws_opened, ws_id}
        {type: ws_reject, ws_id, error}
        {type: ws_frame,  ws_id, data, binary}
        {type: ws_close,  ws_id}
    """

    def __init__(self, ws: web.WebSocketResponse, name: str):
        self.ws = ws
        self.name = name
        self._http: Dict[str, asyncio.Future] = {}
        self._ws_open: Dict[str, asyncio.Future] = {}
        self._ws_queues: Dict[str, asyncio.Queue] = {}

    def dispatch(self, msg: dict) -> None:
        t = msg["type"]
        if t == "http_res":
            f = self._http.pop(msg["req_id"], None)
            if f and not f.done():
                f.set_result(msg)
        elif t == "ws_opened":
            f = self._ws_open.pop(msg["ws_id"], None)
            if f and not f.done():
                f.set_result(True)
        elif t == "ws_reject":
            f = self._ws_open.pop(msg["ws_id"], None)
            if f and not f.done():
                f.set_exception(RuntimeError(msg.get("error", "ws_reject")))
        elif t == "ws_frame":
            q = self._ws_queues.get(msg["ws_id"])
            if q:
                q.put_nowait(msg)
        elif t == "ws_close":
            q = self._ws_queues.pop(msg["ws_id"], None)
            if q:
                q.put_nowait(None)  # sentinel

    async def http(self, method: str, path: str, body=None) -> dict:
        req_id = str(uuid.uuid4())
        f = asyncio.get_running_loop().create_future()
        self._http[req_id] = f
        payload = body if isinstance(body, str) else (json.dumps(body) if body else None)
        await self.ws.send_json({
            "type": "http_req",
            "req_id": req_id,
            "method": method,
            "path": path,
            "headers": {},
            "body": payload,
        })
        return await asyncio.wait_for(f, timeout=30.0)

    async def ws_open(self, ws_id: str, path: str) -> asyncio.Queue:
        f = asyncio.get_running_loop().create_future()
        self._ws_open[ws_id] = f
        q: asyncio.Queue = asyncio.Queue()
        self._ws_queues[ws_id] = q
        await self.ws.send_json({"type": "ws_open", "ws_id": ws_id, "path": path})
        await asyncio.wait_for(f, timeout=15.0)
        return q

    async def ws_send(self, ws_id: str, data, binary: bool = False) -> None:
        if binary:
            await self.ws.send_json({
                "type": "ws_frame", "ws_id": ws_id,
                "data": base64.b64encode(data).decode(), "binary": True,
            })
        else:
            await self.ws.send_json({
                "type": "ws_frame", "ws_id": ws_id,
                "data": data, "binary": False,
            })

    async def ws_close(self, ws_id: str) -> None:
        self._ws_queues.pop(ws_id, None)
        try:
            await self.ws.send_json({"type": "ws_close", "ws_id": ws_id})
        except Exception:
            pass


# ── Hub ──────────────────────────────────────────────────────────────────────

class Hub:
    def __init__(self, token: Optional[str] = None):
        self.token = token
        self.tunnels: Dict[str, TunnelConnection] = {}       # name → tunnel
        self.kernel_tunnel: Dict[str, TunnelConnection] = {} # kernel_id → tunnel

    def _check_auth(self, request: web.Request) -> bool:
        if not self.token:
            return True
        auth = request.headers.get("Authorization", "")
        return auth == f"token {self.token}"

    # ── Tunnel registration ──────────────────────────────────────────────────

    async def handle_tunnel(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        tunnel: Optional[TunnelConnection] = None
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data["type"] == "register":
                        if self.token and data.get("token") != self.token:
                            await ws.send_json({"type": "error", "message": "unauthorized"})
                            break
                        name = data["name"]
                        # Check for duplicate agent name
                        if name in self.tunnels:
                            await ws.send_json({"type": "error", "message": f"agent name '{name}' already registered"})
                            print(f"[Hub] x {name} (rejected: duplicate name)")
                            break
                        tunnel = TunnelConnection(ws, name)
                        self.tunnels[name] = tunnel
                        print(f"[Hub] + {name}")
                        await ws.send_json({"type": "registered"})
                    elif tunnel:
                        tunnel.dispatch(data)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
        finally:
            if tunnel:
                self.tunnels.pop(tunnel.name, None)
                # Clean up kernel mappings belonging to this tunnel
                stale = [kid for kid, t in self.kernel_tunnel.items() if t is tunnel]
                for kid in stale:
                    self.kernel_tunnel.pop(kid, None)
                print(f"[Hub] - {tunnel.name}")
        return ws

    # ── Jupyter Gateway API ──────────────────────────────────────────────────

    async def api_kernelspecs(self, request: web.Request) -> web.Response:
        tasks = {
            name: asyncio.create_task(t.http("GET", "/api/kernelspecs"))
            for name, t in list(self.tunnels.items())
        }
        specs: dict = {}
        for name, task in tasks.items():
            try:
                res = await task
                remote = json.loads(res["body"]).get("kernelspecs", {})
                for k, v in remote.items():
                    key = f"{name}:{k}"
                    v = dict(v)
                    v["name"] = key
                    v.setdefault("spec", {})
                    v["spec"]["display_name"] = f"[{name}] {v['spec'].get('display_name', k)}"
                    specs[key] = v
            except Exception as e:
                print(f"[Hub] kernelspecs from {name}: {e}")
        default = next(iter(specs), "")
        return web.json_response({"default": default, "kernelspecs": specs})

    async def api_kernels_list(self, request: web.Request) -> web.Response:
        results = []
        for t in list(self.tunnels.values()):
            try:
                res = await t.http("GET", "/api/kernels")
                results.extend(json.loads(res["body"]))
            except Exception:
                pass
        return web.json_response(results)

    async def api_kernels_create(self, request: web.Request) -> web.Response:
        body = await request.json()
        kernel_name: str = body.get("name", "")

        if ":" in kernel_name:
            machine, real_name = kernel_name.split(":", 1)
            tunnel = self.tunnels.get(machine)
        else:
            tunnel = next(iter(self.tunnels.values()), None)
            real_name = kernel_name

        if not tunnel:
            return web.json_response({"message": "No remote agent connected"}, status=503)

        body = dict(body, name=real_name)
        res = await tunnel.http("POST", "/api/kernels", body=body)
        kernel = json.loads(res["body"])
        self.kernel_tunnel[kernel["id"]] = tunnel
        return web.json_response(kernel, status=res["status"])

    async def api_kernel_get(self, request: web.Request) -> web.Response:
        kid = request.match_info["kernel_id"]
        tunnel = self.kernel_tunnel.get(kid)
        if not tunnel:
            raise web.HTTPNotFound()
        res = await tunnel.http("GET", f"/api/kernels/{kid}")
        return web.json_response(json.loads(res["body"]), status=res["status"])

    async def api_kernel_delete(self, request: web.Request) -> web.Response:
        kid = request.match_info["kernel_id"]
        tunnel = self.kernel_tunnel.pop(kid, None)
        if not tunnel:
            raise web.HTTPNotFound()
        res = await tunnel.http("DELETE", f"/api/kernels/{kid}")
        return web.Response(status=res["status"])

    async def api_kernel_restart(self, request: web.Request) -> web.Response:
        kid = request.match_info["kernel_id"]
        tunnel = self.kernel_tunnel.get(kid)
        if not tunnel:
            raise web.HTTPNotFound()
        res = await tunnel.http("POST", f"/api/kernels/{kid}/restart", body={})
        return web.json_response(json.loads(res["body"]), status=res["status"])

    async def api_kernel_interrupt(self, request: web.Request) -> web.Response:
        kid = request.match_info["kernel_id"]
        tunnel = self.kernel_tunnel.get(kid)
        if not tunnel:
            raise web.HTTPNotFound()
        res = await tunnel.http("POST", f"/api/kernels/{kid}/interrupt", body={})
        return web.Response(status=res["status"])

    async def api_kernelspec_get(self, request: web.Request) -> web.Response:
        kernel_name: str = request.match_info["kernel_name"]
        if ":" in kernel_name:
            machine, real_name = kernel_name.split(":", 1)
            tunnel = self.tunnels.get(machine)
        else:
            tunnel = next(iter(self.tunnels.values()), None)
            real_name = kernel_name
        if not tunnel:
            raise web.HTTPNotFound()
        res = await tunnel.http("GET", f"/api/kernelspecs/{real_name}")
        if res["status"] == 404:
            raise web.HTTPNotFound()
        spec = json.loads(res["body"])
        spec.setdefault("spec", {})
        spec["spec"]["display_name"] = f"[{tunnel.name}] {spec['spec'].get('display_name', real_name)}"
        return web.json_response(spec, status=res["status"])

    async def api_kernelspec_resource(self, request: web.Request) -> web.Response:
        kernel_name: str = request.match_info["kernel_name"]
        resource_path: str = request.match_info["resource_path"]
        if ":" in kernel_name:
            machine, real_name = kernel_name.split(":", 1)
            tunnel = self.tunnels.get(machine)
        else:
            tunnel = next(iter(self.tunnels.values()), None)
            real_name = kernel_name
        if not tunnel:
            raise web.HTTPNotFound()
        res = await tunnel.http("GET", f"/kernelspecs/{real_name}/{resource_path}")
        if res["status"] == 404:
            raise web.HTTPNotFound()
        body = base64.b64decode(res["body"]) if res.get("binary") else (res["body"] or b"").encode()
        content_type = res.get("headers", {}).get("Content-Type", "application/octet-stream")
        return web.Response(body=body, status=res["status"], content_type=content_type)

    async def api_kernel_channels(self, request: web.Request) -> web.WebSocketResponse:
        kid = request.match_info["kernel_id"]
        tunnel = self.kernel_tunnel.get(kid)
        if not tunnel:
            raise web.HTTPNotFound()

        # Accept WebSocket from JupyterLab
        client_ws = web.WebSocketResponse()
        await client_ws.prepare(request)

        ws_id = str(uuid.uuid4())
        path = f"/api/kernels/{kid}/channels"
        if request.query_string:
            path += f"?{request.query_string}"

        # Ask remote to open a WebSocket to its local Jupyter Server
        queue = await tunnel.ws_open(ws_id, path)

        async def relay_remote_to_client():
            while True:
                frame = await queue.get()
                if frame is None:
                    await client_ws.close()
                    return
                try:
                    if frame["binary"]:
                        await client_ws.send_bytes(base64.b64decode(frame["data"]))
                    else:
                        await client_ws.send_str(frame["data"])
                except Exception:
                    return

        relay = asyncio.create_task(relay_remote_to_client())
        try:
            async for msg in client_ws:
                if msg.type == WSMsgType.TEXT:
                    await tunnel.ws_send(ws_id, msg.data)
                elif msg.type == WSMsgType.BINARY:
                    await tunnel.ws_send(ws_id, msg.data, binary=True)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
        finally:
            relay.cancel()
            await tunnel.ws_close(ws_id)

        return client_ws

    # ── App factory ──────────────────────────────────────────────────────────

    async def debug_tunnels(self, request: web.Request) -> web.Response:
        return web.json_response({
            "tunnels": list(self.tunnels.keys()),
            "kernels": list(self.kernel_tunnel.keys()),
        })

    def make_app(self) -> web.Application:
        @web.middleware
        async def auth_middleware(request: web.Request, handler):
            if self.token and request.path.startswith("/api/"):
                if not self._check_auth(request):
                    raise web.HTTPUnauthorized(
                        text='{"message": "Unauthorized"}',
                        content_type="application/json",
                    )
            return await handler(request)

        app = web.Application(middlewares=[auth_middleware])
        app.router.add_get("/debug/tunnels",                      self.debug_tunnels)
        app.router.add_get("/tunnel/register",                    self.handle_tunnel)
        app.router.add_get("/api/kernelspecs",                              self.api_kernelspecs)
        app.router.add_get("/api/kernelspecs/{kernel_name}",               self.api_kernelspec_get)
        app.router.add_get("/kernelspecs/{kernel_name}/{resource_path:.*}", self.api_kernelspec_resource)
        app.router.add_get("/api/kernels",                                  self.api_kernels_list)
        app.router.add_post("/api/kernels",                                 self.api_kernels_create)
        app.router.add_get("/api/kernels/{kernel_id}",                      self.api_kernel_get)
        app.router.add_delete("/api/kernels/{kernel_id}",                   self.api_kernel_delete)
        app.router.add_post("/api/kernels/{kernel_id}/restart",             self.api_kernel_restart)
        app.router.add_post("/api/kernels/{kernel_id}/interrupt",           self.api_kernel_interrupt)
        app.router.add_get("/api/kernels/{kernel_id}/channels",             self.api_kernel_channels)
        return app

    def run(self, host: str = "0.0.0.0", port: int = 8765) -> None:
        print(f"[Hub] Listening on {host}:{port}")
        if self.token:
            print(f"[Hub] Auth enabled — token: {self.token}")
            print(f"[Hub] JupyterLab config: --GatewayClient.url=http://<host>:{port} --GatewayClient.auth_token={self.token}")
        else:
            print(f"[Hub] Auth disabled (no token)")
            print(f"[Hub] JupyterLab config: --GatewayClient.url=http://<host>:{port}")
        web.run_app(self.make_app(), host=host, port=port, print=None)
