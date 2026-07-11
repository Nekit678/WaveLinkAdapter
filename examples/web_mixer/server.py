"""Example WebSocket/JSON-RPC gateway for browser clients.

The gateway owns one long-lived :class:`WaveLinkClient`. Browser requests are
forwarded to Wave Link without changing their method names or parameter wire
format. Wave Link notifications are broadcast to every connected browser.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import mimetypes
import re
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable, Sequence
from urllib.parse import unquote, urlsplit

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Request, Response
from websockets.typing import Origin

from wavelink_core import (
    ConnectionState,
    WaveLinkClient,
    WaveLinkDisconnectedError,
    WaveLinkProtocolError,
    WaveLinkRpcError,
    WaveLinkTimeoutError,
)
from wavelink_types import JsonModel, JsonValue


JsonRpcId = str | int | None
JsonObject = dict[str, Any]

LOCAL_BROWSER_ORIGIN = re.compile(
    r"https?://(?:localhost|127\.0\.0\.1|\[::1\]|10(?:\.\d{1,3}){3}|"
    r"192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})"
    r"(?::\d+)?\Z"
)
DEFAULT_ORIGINS: tuple[Origin | re.Pattern[str] | None, ...] = (
    None,
    LOCAL_BROWSER_ORIGIN,
)
DEFAULT_WEB_ROOT = Path(__file__).with_name("web")


@dataclass(slots=True, eq=False)
class _ClientSession:
    connection: ServerConnection
    max_concurrent_requests: int
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)
    semaphore: asyncio.Semaphore = field(init=False)

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    async def send(self, payload: JsonObject | list[JsonObject]) -> None:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        async with self.send_lock:
            await self.connection.send(raw)


class WaveLinkWebSocketServer:
    """Expose a shared Wave Link connection to local WebSocket clients."""

    EVENT_METHODS = tuple(sorted(WaveLinkClient.TYPED_EVENT_METHODS))

    def __init__(
        self,
        client: WaveLinkClient | None = None,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        origins: Sequence[Origin | re.Pattern[str] | None] | None = DEFAULT_ORIGINS,
        connect_retry_delay: float = 2.0,
        event_queue_size: int = 512,
        max_concurrent_requests: int = 32,
        max_message_size: int = 1_048_576,
        web_root: str | Path | None = DEFAULT_WEB_ROOT,
    ) -> None:
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if connect_retry_delay <= 0:
            raise ValueError("connect_retry_delay must be greater than zero")
        if event_queue_size <= 0:
            raise ValueError("event_queue_size must be greater than zero")
        if max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be greater than zero")
        if max_message_size <= 0:
            raise ValueError("max_message_size must be greater than zero")

        self.client = client or WaveLinkClient()
        self.host = host
        self.port = port
        self.origins = origins
        self.connect_retry_delay = connect_retry_delay
        self.event_queue_size = event_queue_size
        self.max_concurrent_requests = max_concurrent_requests
        self.max_message_size = max_message_size
        self.web_root = Path(web_root).resolve() if web_root is not None else None

        self._server: Server | None = None
        self._sessions: set[_ClientSession] = set()
        self._event_queue: asyncio.Queue[JsonObject] | None = None
        self._event_task: asyncio.Task[None] | None = None
        self._connect_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._handlers_registered = False
        self._forward_handlers: dict[
            str, Callable[[dict[str, Any]], None]
        ] = {
            method: self._build_event_handler(method) for method in self.EVENT_METHODS
        }
        self._logger = logging.getLogger(__name__)

    @property
    def bound_port(self) -> int | None:
        """Actual listening port; useful when ``port=0`` selected a free port."""
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        """Start accepting web clients and connect to Wave Link in background."""
        if self._server is not None:
            return

        self._stopping.clear()
        self._event_queue = asyncio.Queue(self.event_queue_size)
        self._register_event_handlers()
        try:
            self._server = await serve(
                self._handle_connection,
                self.host,
                self.port,
                origins=self.origins,
                max_size=self.max_message_size,
                process_request=self._process_http_request,
                ping_interval=20,
                ping_timeout=20,
            )
        except BaseException:
            self._unregister_event_handlers()
            self._event_queue = None
            raise

        self._event_task = asyncio.create_task(
            self._broadcast_events(), name="WaveLinkWebEventBroadcaster"
        )
        self._connect_task = asyncio.create_task(
            self._monitor_connection(), name="WaveLinkConnectionMonitor"
        )

    async def close(self) -> None:
        """Stop the listener, clients, background work, and the core client."""
        self._stopping.set()

        background = tuple(
            task
            for task in (self._connect_task, self._event_task)
            if task is not None
        )
        self._connect_task = None
        self._event_task = None
        for task in background:
            task.cancel()

        server = self._server
        self._server = None
        if server is not None:
            server.close(close_connections=True)
            await server.wait_closed()

        if background:
            await asyncio.gather(*background, return_exceptions=True)

        self._sessions.clear()
        self._event_queue = None
        self._unregister_event_handlers()
        await self.client.close()

    async def serve_forever(self) -> None:
        await self.start()
        assert self._server is not None
        try:
            await self._server.serve_forever()
        finally:
            await self.close()

    async def _monitor_connection(self) -> None:
        initially_connected = self.client.state is ConnectionState.CONNECTED
        last_status = tuple(self._status().values())
        while not self._stopping.is_set():
            if not initially_connected:
                try:
                    await self.client.connect()
                    initially_connected = True
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._logger.warning("Cannot connect to Wave Link: %s", exc)

            status = self._status()
            signature = tuple(status.values())
            if signature != last_status:
                last_status = signature
                await self._enqueue_notification("server.connectionChanged", status)

            delay = 0.25 if initially_connected else self.connect_retry_delay
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=delay)
            except TimeoutError:
                pass

    def _register_event_handlers(self) -> None:
        if self._handlers_registered:
            return
        for method in self.EVENT_METHODS:
            self.client.on(method, self._forward_handlers[method])
        self._handlers_registered = True

    def _unregister_event_handlers(self) -> None:
        if not self._handlers_registered:
            return
        for method in self.EVENT_METHODS:
            self.client.off(method, self._forward_handlers[method])
        self._handlers_registered = False

    def _build_event_handler(
        self, method: str
    ) -> Callable[[dict[str, Any]], None]:
        def forward(params: dict[str, Any]) -> None:
            queue = self._event_queue
            if queue is None:
                return
            notification = {"jsonrpc": "2.0", "method": method, "params": params}
            try:
                queue.put_nowait(notification)
            except asyncio.QueueFull:
                self._logger.warning(
                    "Dropping web event %s because the event queue is full", method
                )

        return forward

    async def _enqueue_notification(self, method: str, params: JsonObject) -> None:
        queue = self._event_queue
        if queue is None:
            return
        notification = {"jsonrpc": "2.0", "method": method, "params": params}
        try:
            queue.put_nowait(notification)
        except asyncio.QueueFull:
            self._logger.warning("Dropping web notification %s", method)

    async def _broadcast_events(self) -> None:
        queue = self._event_queue
        if queue is None:
            return
        while True:
            payload = await queue.get()
            try:
                sessions = tuple(self._sessions)
                if sessions:
                    await asyncio.gather(
                        *(self._safe_send(session, payload) for session in sessions)
                    )
            finally:
                queue.task_done()

    async def _safe_send(
        self, session: _ClientSession, payload: JsonObject | list[JsonObject]
    ) -> None:
        try:
            await session.send(payload)
        except Exception:
            self._sessions.discard(session)

    async def _handle_connection(self, connection: ServerConnection) -> None:
        session = _ClientSession(connection, self.max_concurrent_requests)
        self._sessions.add(session)
        try:
            await session.send(
                {
                    "jsonrpc": "2.0",
                    "method": "server.connectionChanged",
                    "params": self._status(),
                }
            )
            async for raw in connection:
                task = asyncio.create_task(self._handle_message(session, raw))
                session.tasks.add(task)
                task.add_done_callback(session.tasks.discard)
        finally:
            self._sessions.discard(session)
            for task in tuple(session.tasks):
                task.cancel()
            if session.tasks:
                await asyncio.gather(*session.tasks, return_exceptions=True)

    def _process_http_request(
        self, connection: ServerConnection, request: Request
    ) -> Response | None:
        """Serve the bundled UI while leaving WebSocket upgrades untouched."""
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return None

        web_root = self.web_root
        if web_root is None:
            return self._http_response(HTTPStatus.NOT_FOUND, b"Not found\n", "text/plain")

        path = unquote(urlsplit(request.path).path)
        if path in ("", "/"):
            path = "/index.html"
        relative = path.lstrip("/")
        candidate = (web_root / relative).resolve()
        try:
            candidate.relative_to(web_root)
        except ValueError:
            return self._http_response(HTTPStatus.FORBIDDEN, b"Forbidden\n", "text/plain")

        if not candidate.is_file():
            return self._http_response(HTTPStatus.NOT_FOUND, b"Not found\n", "text/plain")

        try:
            body = candidate.read_bytes()
        except OSError:
            self._logger.exception("Cannot read web asset: %s", candidate)
            return self._http_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                b"Internal server error\n",
                "text/plain",
            )

        content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        return self._http_response(
            HTTPStatus.OK, body, content_type, cache_control="no-cache"
        )

    @staticmethod
    def _http_response(
        status: HTTPStatus,
        body: bytes,
        content_type: str,
        *,
        cache_control: str = "no-store",
    ) -> Response:
        headers = Headers(
            [
                ("Content-Type", f"{content_type}; charset=utf-8"),
                ("Content-Length", str(len(body))),
                ("Cache-Control", cache_control),
                ("X-Content-Type-Options", "nosniff"),
            ]
        )
        return Response(status.value, status.phrase, headers, body)

    async def _handle_message(self, session: _ClientSession, raw: str | bytes) -> None:
        async with session.semaphore:
            response = await self.process_message(raw)
            if response is not None:
                await self._safe_send(session, response)

    async def process_message(
        self, raw: str | bytes
    ) -> JsonObject | list[JsonObject] | None:
        """Process one JSON-RPC message; public to simplify transport-free tests."""
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
            return self._error(None, -32700, "Parse error")

        if isinstance(payload, list):
            if not payload:
                return self._error(None, -32600, "Invalid Request")
            responses = await asyncio.gather(
                *(self._process_request(item) for item in payload)
            )
            return [response for response in responses if response is not None] or None
        return await self._process_request(payload)

    async def _process_request(self, request: Any) -> JsonObject | None:
        if not isinstance(request, dict):
            return self._error(None, -32600, "Invalid Request")

        request_id: JsonRpcId = request.get("id")
        has_id = "id" in request
        if (
            request.get("jsonrpc") != "2.0"
            or not isinstance(request.get("method"), str)
            or not request["method"]
            or (
                has_id
                and (
                    isinstance(request_id, bool)
                    or not isinstance(request_id, (str, int, type(None)))
                )
            )
            or (
                "params" in request
                and not isinstance(request["params"], (dict, list, type(None)))
            )
        ):
            return self._error(request_id if has_id else None, -32600, "Invalid Request")

        method = request["method"]
        params = request.get("params")
        try:
            result = await self._dispatch(method, params)
            response: JsonObject = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result,
            }
        except WaveLinkRpcError as exc:
            response = self._error(
                request_id,
                exc.code if exc.code is not None else -32000,
                exc.message,
                exc.data,
            )
        except WaveLinkDisconnectedError as exc:
            response = self._error(request_id, -32001, str(exc))
        except WaveLinkTimeoutError as exc:
            response = self._error(request_id, -32002, str(exc))
        except WaveLinkProtocolError as exc:
            response = self._error(request_id, -32003, str(exc))
        except (ConnectionError, OSError) as exc:
            response = self._error(request_id, -32004, str(exc))
        except (TypeError, ValueError) as exc:
            response = self._error(request_id, -32602, str(exc))
        except Exception:
            self._logger.exception("Unhandled JSON-RPC method failure: %s", method)
            response = self._error(request_id, -32603, "Internal error")

        return response if has_id else None

    async def _dispatch(self, method: str, params: Any) -> JsonValue:
        if method == "server.getStatus":
            self._require_empty_params(params)
            return self._status()
        if method == "server.getState":
            self._require_empty_params(params)
            return self._state_snapshot()
        if method == "server.refreshState":
            self._require_empty_params(params)
            await asyncio.gather(
                self.client.get_application_info(),
                self.client.get_input_devices(),
                self.client.get_output_devices(),
                self.client.get_channels(),
                self.client.get_mixes(),
            )
            return self._state_snapshot()
        if method == "server.ping":
            self._require_empty_params(params)
            return "pong"
        if method.startswith("server."):
            raise WaveLinkRpcError("Method not found", code=-32601, method=method)
        return await self.client.call(method, params)

    @staticmethod
    def _require_empty_params(params: Any) -> None:
        if params not in (None, {}, []):
            raise ValueError("This method doesn't accept params")

    def _status(self) -> JsonObject:
        connected = (
            self.client.state is ConnectionState.CONNECTED
            and self.client.ws is not None
        )
        return {
            "connected": connected,
            "state": self.client.state.name.lower(),
            "waveLinkHost": self.client.host,
            "waveLinkPort": self.client.connected_port,
        }

    def _state_snapshot(self) -> JsonObject:
        def encode(model: JsonModel | None) -> JsonObject | None:
            return model.to_dict() if model is not None else None

        return {
            "connection": self._status(),
            "applicationInfo": encode(self.client.application_info),
            "inputDevices": [item.to_dict() for item in self.client.input_devices],
            "outputDevices": [item.to_dict() for item in self.client.output_devices],
            "mainOutput": encode(self.client.main_output),
            "channels": [item.to_dict() for item in self.client.channels],
            "mixes": [item.to_dict() for item in self.client.mixes],
            "levelMeters": encode(self.client.level_meters),
            "focusedApp": encode(self.client.focused_app),
        }

    @staticmethod
    def _error(
        request_id: JsonRpcId,
        code: int,
        message: str,
        data: Any = None,
    ) -> JsonObject:
        error: JsonObject = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local WebSocket/JSON-RPC gateway for Elgato Wave Link"
    )
    parser.add_argument("--host", default="127.0.0.1", help="listen host")
    parser.add_argument("--port", default=8765, type=int, help="listen port")
    parser.add_argument(
        "--wavelink-host", default="127.0.0.1", help="Wave Link RPC host"
    )
    parser.add_argument(
        "--allow-origin",
        action="append",
        default=None,
        help="allowed browser Origin; repeat as needed, or use '*'",
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--no-web-ui",
        action="store_true",
        help="disable the bundled web interface and serve WebSocket only",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> None:
    origins: Sequence[Origin | re.Pattern[str] | None] | None
    if args.allow_origin is None:
        origins = DEFAULT_ORIGINS
    elif "*" in args.allow_origin:
        origins = None
    else:
        origins = tuple(args.allow_origin)

    client = WaveLinkClient(host=args.wavelink_host, debug=args.debug)
    gateway = WaveLinkWebSocketServer(
        client,
        host=args.host,
        port=args.port,
        origins=origins,
        web_root=None if args.no_web_ui else DEFAULT_WEB_ROOT,
    )
    await gateway.start()
    logging.getLogger(__name__).info(
        "Wave Link mixer is available at http://%s:%s",
        args.host,
        gateway.bound_port,
    )
    try:
        await asyncio.Future()
    finally:
        await gateway.close()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
