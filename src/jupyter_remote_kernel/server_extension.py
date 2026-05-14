"""
Jupyter Server Extension: embeds the Hub inside JupyterLab.

Routes mounted at /jrk/, protected by JupyterLab's own token auth:
  GET  /jrk/tunnel/register              — agent WebSocket tunnel
  GET  /jrk/api/kernelspecs              — aggregated kernelspecs
  GET  /jrk/api/kernelspecs/{name}       — single kernelspec
  GET  /jrk/kernelspecs/{name}/{res}     — kernelspec resource
  GET  /jrk/api/kernels                  — list kernels
  POST /jrk/api/kernels                  — create kernel
  GET  /jrk/api/kernels/{id}             — get kernel
  DELETE /jrk/api/kernels/{id}           — delete kernel
  POST /jrk/api/kernels/{id}/restart     — restart
  POST /jrk/api/kernels/{id}/interrupt   — interrupt
  GET  /jrk/api/kernels/{id}/channels    — kernel WebSocket

JupyterLab config (no separate Hub process needed):
    jupyter lab \\
        --GatewayClient.url=http://localhost:<port>/jrk \\
        --GatewayClient.auth_token=<same-as-ServerApp.token>
"""

import asyncio
import base64
import json
import time
import uuid
from typing import Dict, Optional

import tornado.web
import tornado.websocket
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.extension.application import ExtensionApp


# ── Compatibility patches for GatewayKernelClient ─────────────────────────────
#
# jupyter_server_nbmodel calls GatewayKernelClient.start_channels() +
# execute_interactive() from within the same Tornado event loop that serves
# the /jrk/ WebSocket endpoints.  Two bugs prevent this from working:
#
#   1. start_channels() calls websocket.create_connection() synchronously,
#      blocking the event loop. Tornado can't respond to the WS upgrade while
#      the loop is blocked → deadlock → WebSocketTimeoutException.
#      Fix: run create_connection() in a thread executor.
#
#   2. _async_execute_interactive() uses zmq.asyncio.Poller which requires a
#      ZMQ .socket attribute on each channel. GatewayKernelClient channels are
#      ChannelQueue objects (WebSocket-backed) with no .socket attribute.
#      Fix: replace the ZMQ poll loop with ChannelQueue.get_msg() calls.
#
# Both fixes live here so they are version-controlled with JRK and survive
# upstream upgrades of jupyter_server.

def _patch_gateway_kernel_client():
    try:
        import websocket as _ws
        from jupyter_server.gateway.managers import GatewayKernelClient, GatewayClient
        from jupyter_core.utils import ensure_async
    except ImportError:
        return  # jupyter_server_nbmodel not installed; nothing to patch

    # ── Patch 1: non-blocking start_channels ──────────────────────────────────

    _orig_start_channels = GatewayKernelClient.start_channels

    async def _start_channels_nonblocking(self, shell=True, iopub=True, stdin=True,
                                          hb=True, control=True):
        from urllib.parse import quote as url_escape
        from jupyter_server.utils import url_path_join
        gc = GatewayClient.instance()
        ws_url = url_path_join(
            gc.ws_url or "",
            gc.kernels_endpoint,
            url_escape(self.kernel_id),
            "channels",
        )
        ssl_options = {
            "ca_certs": gc.ca_certs,
            "certfile": gc.client_cert,
            "keyfile": gc.client_key,
        }
        timeout = gc.KERNEL_LAUNCH_TIMEOUT
        auth_header = {gc.auth_header_key: f"{gc.auth_scheme} {gc.auth_token}"}
        loop = asyncio.get_event_loop()
        self.channel_socket = await loop.run_in_executor(
            None,
            lambda: _ws.create_connection(
                ws_url, timeout=timeout, enable_multithread=True,
                sslopt=ssl_options, header=auth_header,
            ),
        )
        await ensure_async(
            super(GatewayKernelClient, self).start_channels(
                shell=shell, iopub=iopub, stdin=stdin, hb=hb, control=control)
        )
        from threading import Thread
        self.response_router = Thread(target=self._route_responses)
        self.response_router.start()

    GatewayKernelClient.start_channels = _start_channels_nonblocking

    # ── Patch 2: ChannelQueue-compatible execute_interactive ──────────────────

    async def _execute_interactive_via_queue(
        self, code, silent=False, store_history=True, user_expressions=None,
        allow_stdin=None, stop_on_error=True, timeout=None,
        output_hook=None, stdin_hook=None,
    ):
        """Use ChannelQueue.get_msg() instead of ZMQ Poller."""
        if user_expressions is None:
            user_expressions = {}
        if allow_stdin is None:
            allow_stdin = self.allow_stdin

        msg_id = await ensure_async(
            self.execute(code, silent=silent, store_history=store_history,
                         user_expressions=user_expressions, allow_stdin=allow_stdin,
                         stop_on_error=stop_on_error)
        )

        if output_hook is None:
            output_hook = self._output_hook_default

        deadline = (time.monotonic() + timeout) if timeout is not None else None

        while True:
            remaining = max(0.0, deadline - time.monotonic()) if deadline else None
            try:
                msg = await self.iopub_channel.get_msg(timeout=remaining)
            except Exception:
                raise TimeoutError("Timeout waiting for kernel output")

            if msg["parent_header"].get("msg_id") != msg_id:
                continue
            output_hook(msg)
            if (msg["header"]["msg_type"] == "status"
                    and msg["content"]["execution_state"] == "idle"):
                break

        remaining = max(0.0, deadline - time.monotonic()) if deadline else None
        return await self._async_recv_reply(msg_id, timeout=remaining)

    # AsyncKernelClient assigns execute_interactive as a direct class attribute
    # (execute_interactive = KernelClient._async_execute_interactive), bypassing
    # MRO. We must override the attribute explicitly, not just the method.
    GatewayKernelClient._async_execute_interactive = _execute_interactive_via_queue
    GatewayKernelClient.execute_interactive = _execute_interactive_via_queue


_patch_gateway_kernel_client()


# ── Shared state ──────────────────────────────────────────────────────────────

class HubState:
    def __init__(self):
        self.tunnels: Dict[str, "AgentTunnelHandler"] = {}
        self.kernel_tunnel: Dict[str, "AgentTunnelHandler"] = {}


# ── Agent tunnel WebSocket handler ────────────────────────────────────────────

class AgentTunnelHandler(JupyterHandler, tornado.websocket.WebSocketHandler):
    """Persistent WS from one remote agent.  Doubles as the TunnelConnection."""

    def initialize(self, hub_state: HubState):
        super().initialize()
        self.hub_state = hub_state
        self.name: Optional[str] = None
        self._http: Dict[str, asyncio.Future] = {}
        self._ws_open: Dict[str, asyncio.Future] = {}
        self._ws_queues: Dict[str, asyncio.Queue] = {}

    def check_origin(self, origin: str) -> bool:
        return True  # auth is handled by @authenticated on get()

    @tornado.web.authenticated
    def get(self, *args, **kwargs):
        """HTTP upgrade; auth checked here so the WS is already authenticated."""
        return super().get(*args, **kwargs)

    def open(self):
        pass  # wait for register message

    def on_message(self, raw: str) -> None:
        data = json.loads(raw)
        t = data.get("type")
        if t == "register":
            name = data["name"]
            # Check for duplicate agent name
            if name in self.hub_state.tunnels:
                self.write_message(json.dumps({"type": "error", "message": f"agent name '{name}' already registered"}))
                print(f"[JRK] x {name} (rejected: duplicate name)")
                self.close(4409, f"agent name '{name}' already registered")
                return
            self.name = name
            self.hub_state.tunnels[self.name] = self
            print(f"[JRK] + {self.name}")
            self.write_message(json.dumps({"type": "registered"}))
            asyncio.create_task(self._restore_kernels())
        elif self.name:
            self._dispatch(data)

    def on_close(self) -> None:
        if self.name:
            self.hub_state.tunnels.pop(self.name, None)
            dead = [k for k, t in list(self.hub_state.kernel_tunnel.items()) if t is self]
            for k in dead:
                self.hub_state.kernel_tunnel.pop(k, None)
            print(f"[JRK] - {self.name}")

    # ── Dispatch incoming frames from agent ───────────────────────────────────

    def _dispatch(self, msg: dict) -> None:
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
                q.put_nowait(None)

    # ── Outbound: relay to agent ──────────────────────────────────────────────

    async def relay_http(self, method: str, path: str, body=None) -> dict:
        req_id = str(uuid.uuid4())
        f = asyncio.get_running_loop().create_future()
        self._http[req_id] = f
        payload = body if isinstance(body, str) else (json.dumps(body) if body else None)
        self.write_message(json.dumps({
            "type": "http_req", "req_id": req_id,
            "method": method, "path": path,
            "headers": {}, "body": payload,
        }))
        return await asyncio.wait_for(f, timeout=30.0)

    async def _restore_kernels(self) -> None:
        """On reconnect, re-populate kernel_tunnel for kernels still running on this agent."""
        try:
            res = await self.relay_http("GET", "/api/kernels")
            kernels = json.loads(res["body"])
            restored = 0
            for k in kernels:
                kid = k.get("id")
                if kid and kid not in self.hub_state.kernel_tunnel:
                    self.hub_state.kernel_tunnel[kid] = self
                    restored += 1
            if restored:
                print(f"[JRK] restored {restored} kernel(s) for {self.name}")
        except Exception as e:
            print(f"[JRK] failed to restore kernels for {self.name}: {e}")

    async def open_remote_ws(self, ws_id: str, path: str) -> asyncio.Queue:
        f = asyncio.get_running_loop().create_future()
        self._ws_open[ws_id] = f
        q: asyncio.Queue = asyncio.Queue()
        self._ws_queues[ws_id] = q
        self.write_message(json.dumps({"type": "ws_open", "ws_id": ws_id, "path": path}))
        try:
            await asyncio.wait_for(f, timeout=15.0)
        except Exception:
            self._ws_queues.pop(ws_id, None)
            raise
        return q

    def send_ws_frame(self, ws_id: str, data, binary: bool = False) -> None:
        if binary:
            payload = {"type": "ws_frame", "ws_id": ws_id,
                       "data": base64.b64encode(data).decode(), "binary": True}
        else:
            payload = {"type": "ws_frame", "ws_id": ws_id, "data": data, "binary": False}
        self.write_message(json.dumps(payload))

    def close_remote_ws(self, ws_id: str) -> None:
        q = self._ws_queues.pop(ws_id, None)
        if q:
            q.put_nowait(None)  # wake up _relay_from_remote so it exits
        try:
            self.write_message(json.dumps({"type": "ws_close", "ws_id": ws_id}))
        except Exception:
            pass


# ── HTTP handlers ─────────────────────────────────────────────────────────────

class _Base(JupyterHandler):
    @property
    def hub_state(self) -> HubState:
        return self.settings["jrk_hub_state"]

    async def _find_tunnel_for_kernel(self, kernel_id: str) -> Optional["AgentTunnelHandler"]:
        """Search all connected agents for a kernel, restore the mapping if found."""
        for t in list(self.hub_state.tunnels.values()):
            try:
                res = await t.relay_http("GET", f"/api/kernels/{kernel_id}")
                if res["status"] == 200:
                    self.hub_state.kernel_tunnel[kernel_id] = t
                    return t
            except Exception:
                pass
        return None


class KernelSpecsHandler(_Base):
    @tornado.web.authenticated
    async def get(self):
        tasks = {
            name: asyncio.create_task(t.relay_http("GET", "/api/kernelspecs"))
            for name, t in list(self.hub_state.tunnels.items())
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
                print(f"[JRK] kernelspecs from {name}: {e}")
        self.finish({"default": next(iter(specs), ""), "kernelspecs": specs})


class KernelSpecHandler(_Base):
    @tornado.web.authenticated
    async def get(self, kernel_name: str):
        machine, real_name = (kernel_name.split(":", 1) if ":" in kernel_name
                              else (None, kernel_name))
        tunnel = (self.hub_state.tunnels.get(machine) if machine
                  else next(iter(self.hub_state.tunnels.values()), None))
        if not tunnel:
            raise tornado.web.HTTPError(404)
        res = await tunnel.relay_http("GET", f"/api/kernelspecs/{real_name}")
        if res["status"] == 404:
            raise tornado.web.HTTPError(404)
        spec = json.loads(res["body"])
        spec.setdefault("spec", {})
        spec["spec"]["display_name"] = f"[{tunnel.name}] {spec['spec'].get('display_name', real_name)}"
        self.finish(spec)


class KernelSpecResourceHandler(_Base):
    @tornado.web.authenticated
    async def get(self, kernel_name: str, resource_path: str):
        machine, real_name = (kernel_name.split(":", 1) if ":" in kernel_name
                              else (None, kernel_name))
        tunnel = (self.hub_state.tunnels.get(machine) if machine
                  else next(iter(self.hub_state.tunnels.values()), None))
        if not tunnel:
            raise tornado.web.HTTPError(404)
        res = await tunnel.relay_http("GET", f"/kernelspecs/{real_name}/{resource_path}")
        if res["status"] == 404:
            raise tornado.web.HTTPError(404)
        body = base64.b64decode(res["body"]) if res.get("binary") else (res["body"] or "").encode()
        self.set_header("Content-Type",
                        res.get("headers", {}).get("Content-Type", "application/octet-stream"))
        self.finish(body)


class KernelsHandler(_Base):
    @tornado.web.authenticated
    async def get(self):
        results = []
        for t in list(self.hub_state.tunnels.values()):
            try:
                res = await t.relay_http("GET", "/api/kernels")
                for k in json.loads(res["body"]):
                    k["name"] = f"{t.name}:{k['name']}"
                    results.append(k)
            except Exception:
                pass
        self.set_header("Content-Type", "application/json")
        self.finish(json.dumps(results))

    @tornado.web.authenticated
    async def post(self):
        body = json.loads(self.request.body)
        kernel_name: str = body.get("name", "")
        machine, real_name = (kernel_name.split(":", 1) if ":" in kernel_name
                              else (None, kernel_name))
        tunnel = (self.hub_state.tunnels.get(machine) if machine
                  else next(iter(self.hub_state.tunnels.values()), None))
        if not tunnel:
            self.set_status(503)
            self.finish({"message": "No remote agent connected"})
            return
        res = await tunnel.relay_http("POST", "/api/kernels", body=dict(body, name=real_name))
        kernel = json.loads(res["body"])
        self.hub_state.kernel_tunnel[kernel["id"]] = tunnel
        kernel["name"] = f"{tunnel.name}:{kernel['name']}"
        self.set_status(res["status"])
        self.finish(kernel)


class KernelHandler(_Base):
    @tornado.web.authenticated
    async def get(self, kernel_id: str):
        tunnel = self.hub_state.kernel_tunnel.get(kernel_id) or await self._find_tunnel_for_kernel(kernel_id)
        if not tunnel:
            raise tornado.web.HTTPError(404)
        res = await tunnel.relay_http("GET", f"/api/kernels/{kernel_id}")
        self.set_status(res["status"])
        kernel = json.loads(res["body"])
        kernel["name"] = f"{tunnel.name}:{kernel['name']}"
        self.finish(kernel)

    @tornado.web.authenticated
    async def delete(self, kernel_id: str):
        tunnel = self.hub_state.kernel_tunnel.pop(kernel_id, None) or await self._find_tunnel_for_kernel(kernel_id)
        if not tunnel:
            raise tornado.web.HTTPError(404)
        res = await tunnel.relay_http("DELETE", f"/api/kernels/{kernel_id}")
        self.set_status(res["status"])
        self.finish()


class KernelActionHandler(_Base):
    @tornado.web.authenticated
    async def post(self, kernel_id: str, action: str):
        tunnel = self.hub_state.kernel_tunnel.get(kernel_id) or await self._find_tunnel_for_kernel(kernel_id)
        if not tunnel:
            raise tornado.web.HTTPError(404)
        try:
            res = await tunnel.relay_http("POST", f"/api/kernels/{kernel_id}/{action}", body={})
        except Exception:
            raise tornado.web.HTTPError(502, "Tunnel error during kernel action")
        self.set_status(res["status"])
        if action == "restart":
            kernel = json.loads(res["body"])
            kernel["name"] = f"{tunnel.name}:{kernel['name']}"
            self.finish(kernel)
        else:
            self.finish()


class DebugTunnelsHandler(_Base):
    @tornado.web.authenticated
    async def get(self):
        self.finish({
            "tunnels": list(self.hub_state.tunnels.keys()),
            "kernels": list(self.hub_state.kernel_tunnel.keys()),
        })


# ── Kernel channels WebSocket ─────────────────────────────────────────────────

class KernelChannelsHandler(JupyterHandler, tornado.websocket.WebSocketHandler):
    def initialize(self, hub_state: HubState):
        super().initialize()
        self.hub_state = hub_state
        self.tunnel: Optional[AgentTunnelHandler] = None
        self.ws_id: Optional[str] = None

    def check_origin(self, origin: str) -> bool:
        return True

    @tornado.web.authenticated
    def get(self, *args, **kwargs):
        return super().get(*args, **kwargs)

    async def open(self, kernel_id: str):
        tunnel = self.hub_state.kernel_tunnel.get(kernel_id)
        if not tunnel:
            # Lazy restore: search all connected agents
            for t in list(self.hub_state.tunnels.values()):
                try:
                    res = await t.relay_http("GET", f"/api/kernels/{kernel_id}")
                    if res["status"] == 200:
                        self.hub_state.kernel_tunnel[kernel_id] = t
                        tunnel = t
                        break
                except Exception:
                    pass
        if not tunnel:
            self.close(4404, "Kernel not found")
            return
        self.tunnel = tunnel
        self.ws_id = str(uuid.uuid4())
        path = f"/api/kernels/{kernel_id}/channels"
        if self.request.query:
            path += f"?{self.request.query}"
        try:
            queue = await tunnel.open_remote_ws(self.ws_id, path)
        except Exception as e:
            self.close(4500, str(e))
            return
        asyncio.create_task(self._relay_from_remote(queue))

    async def _relay_from_remote(self, queue: asyncio.Queue):
        while True:
            frame = await queue.get()
            if frame is None:
                self.close()
                return
            try:
                if frame["binary"]:
                    self.write_message(base64.b64decode(frame["data"]), binary=True)
                else:
                    self.write_message(frame["data"])
            except Exception:
                return

    def on_message(self, message) -> None:
        if self.tunnel and self.ws_id:
            if isinstance(message, bytes):
                self.tunnel.send_ws_frame(self.ws_id, message, binary=True)
            else:
                self.tunnel.send_ws_frame(self.ws_id, message, binary=False)
        else:
            self.log.warning(
                "KernelChannelsHandler.on_message: message dropped — "
                "tunnel=%s ws_id=%s (race: open() not yet complete)",
                self.tunnel is not None,
                self.ws_id,
            )

    def on_close(self) -> None:
        if self.tunnel and self.ws_id:
            self.tunnel.close_remote_ws(self.ws_id)


# ── Extension App ─────────────────────────────────────────────────────────────

class JupyterRemoteKernelExtension(ExtensionApp):
    name = "jupyter_remote_kernel"

    def initialize_settings(self):
        self.settings["jrk_hub_state"] = HubState()

    def initialize_handlers(self):
        hs = self.settings["jrk_hub_state"]
        kw = {"hub_state": hs}
        self.handlers = [
            (r"/jrk/tunnel/register",                        AgentTunnelHandler,       kw),
            (r"/jrk/debug/tunnels",                          DebugTunnelsHandler),
            (r"/jrk/api/kernelspecs",                        KernelSpecsHandler),
            (r"/jrk/api/kernelspecs/([^/]+)",                KernelSpecHandler),
            (r"/jrk/kernelspecs/([^/]+)/(.*)",               KernelSpecResourceHandler),
            (r"/jrk/api/kernels",                            KernelsHandler),
            (r"/jrk/api/kernels/([^/]+)/channels",           KernelChannelsHandler,    kw),
            (r"/jrk/api/kernels/([^/]+)/(restart|interrupt)", KernelActionHandler),
            (r"/jrk/api/kernels/([^/]+)",                    KernelHandler),
        ]


def _jupyter_server_extension_points():
    return [{"module": "jupyter_remote_kernel.server_extension",
             "app": JupyterRemoteKernelExtension}]


# Legacy alias for older jupyter_server
_jupyter_server_extension_paths = _jupyter_server_extension_points


main = JupyterRemoteKernelExtension.launch_instance
