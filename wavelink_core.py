"""Низкоуровневый WebSocket/JSON-RPC-клиент для Elgato Wave Link 3.x.

Он отвечает за поиск порта, жизненный цикл WebSocket-соединения, сопоставление
JSON-RPC-запросов и ответов, обработку событий и RPC-обёртки Wave Link,
которые используются остальными частями проекта.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
from copy import deepcopy
from dataclasses import dataclass, fields
from enum import Enum, auto
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol, TypeVar

import websockets
from websockets.typing import Origin

from wavelink_types import (
    ApplicationInfo,
    Channel,
    CreateProfileRequested,
    ChannelMixUpdate,
    ChannelUpdate,
    EffectUpdate,
    FocusedAppChanged,
    FocusedAppSubscription,
    InputDevice,
    InputDeviceUpdate,
    InputUpdate,
    JsonModel,
    JsonValue,
    LevelMeterChanged,
    LevelMeterSubscription,
    LevelMeterType,
    LevelValue,
    MainOutput,
    Mix,
    MixUpdate,
    OutputDevice,
    OutputDeviceUpdate,
    OutputDevices,
    OutputDeviceUpdateParams,
    OutputDeviceUpdateResult,
    OutputUpdate,
    PluginInfoResult,
    SetOutputDeviceParams,
    SubscriptionUpdate,
    WaveLinkSchemaError,
)


EventHandler = Callable[[dict[str, Any]], Awaitable[None] | None]
WaveLinkEvent = (
    InputDevice
    | OutputDevice
    | OutputDevices
    | Channel
    | Mix
    | LevelMeterChanged
    | FocusedAppChanged
    | CreateProfileRequested
    | list[InputDevice]
    | list[Channel]
    | list[Mix]
)
TypedEventHandler = Callable[[WaveLinkEvent], Awaitable[None] | None]


class WaveLinkRpcError(RuntimeError):
    """Wave Link вернул ответ с JSON-RPC-ошибкой."""

    def __init__(
        self,
        error: str | dict[str, Any],
        *,
        code: int | None = None,
        data: Any = None,
        method: str | None = None,
        request_id: int | None = None,
    ) -> None:
        if isinstance(error, dict):
            raw_code = error.get("code")
            code = raw_code if isinstance(raw_code, int) else code
            message = str(error.get("message", error))
            data = error.get("data", data)
        else:
            message = str(error)

        self.code = code
        self.message = message
        self.data = data
        self.method = method
        self.request_id = request_id

        details = f"[{code}] {message}" if code is not None else message
        if method is not None:
            details = f"{method}: {details}"
        super().__init__(details)

    @property
    def is_invalid_params(self) -> bool:
        """Показывает, отклонил ли сервер структуру параметров JSON-RPC."""
        return self.code == -32602


class WaveLinkProtocolError(RuntimeError):
    """Ответ Wave Link не соответствует ожидаемому контракту API."""


class WaveLinkDisconnectedError(ConnectionError):
    """WebSocket-соединение закрылось во время выполнения операции."""


class WaveLinkTimeoutError(TimeoutError):
    """Wave Link не ответил на RPC-запрос за отведённое время."""


class ConnectionState(Enum):
    """Состояния жизненного цикла для диагностики и управления клиентом."""

    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    CLOSING = auto()


class WebSocketConnection(Protocol):
    """Минимальная часть WebSocket-интерфейса, необходимая клиенту."""

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...

    async def send(self, message: str) -> None: ...

    async def close(self) -> None: ...


@dataclass(slots=True)
class _PendingRequest:
    future: asyncio.Future[JsonValue]
    method: str
    generation: int


class WaveLinkClient:
    """Асинхронный клиент локального WebSocket/JSON-RPC API Wave Link."""

    FALLBACK_PORTS = list(range(1884, 1894))
    LEVEL_METER_TYPES: tuple[LevelMeterType, ...] = (
        "input",
        "output",
        "channel",
        "mix",
    )
    TYPED_EVENT_METHODS = frozenset(
        {
            "inputDevicesChanged",
            "inputDeviceChanged",
            "outputDevicesChanged",
            "outputDeviceChanged",
            "channelsChanged",
            "channelChanged",
            "mixesChanged",
            "mixChanged",
            "levelMeterChanged",
            "focusedAppChanged",
            "createProfileRequested",
        }
    )

    def __init__(
        self,
        host: str = "127.0.0.1",
        debug: bool = False,
        *,
        rpc_timeout: float | None = 10.0,
        open_timeout: float | None = 3.0,
        close_timeout: float | None = 3.0,
        event_queue_size: int = 256,
        auto_reconnect: bool = True,
        reconnect_initial_delay: float = 0.5,
        reconnect_max_delay: float = 10.0,
        reconnect_backoff: float = 2.0,
    ):
        if rpc_timeout is not None and rpc_timeout <= 0:
            raise ValueError("rpc_timeout must be greater than zero or None")
        if open_timeout is not None and open_timeout <= 0:
            raise ValueError("open_timeout must be greater than zero or None")
        if close_timeout is not None and close_timeout <= 0:
            raise ValueError("close_timeout must be greater than zero or None")
        if event_queue_size <= 0:
            raise ValueError("event_queue_size must be greater than zero")
        if reconnect_initial_delay <= 0:
            raise ValueError("reconnect_initial_delay must be greater than zero")
        if reconnect_max_delay <= 0:
            raise ValueError("reconnect_max_delay must be greater than zero")
        if reconnect_initial_delay > reconnect_max_delay:
            raise ValueError(
                "reconnect_initial_delay cannot exceed reconnect_max_delay"
            )
        if reconnect_backoff < 1:
            raise ValueError("reconnect_backoff must be at least 1")

        self.host = host
        self.debug = debug
        self.rpc_timeout = rpc_timeout
        self.open_timeout = open_timeout
        self.close_timeout = close_timeout
        self.event_queue_size = event_queue_size
        self.auto_reconnect = bool(auto_reconnect)
        self.reconnect_initial_delay = reconnect_initial_delay
        self.reconnect_max_delay = reconnect_max_delay
        self.reconnect_backoff = reconnect_backoff

        self.ws: WebSocketConnection | None = None
        self.connected_port: int | None = None
        self.state = ConnectionState.DISCONNECTED

        self._next_id = 1
        self._pending: dict[int, _PendingRequest] = {}
        self._event_handlers: dict[str, list[EventHandler]] = {}
        self._typed_event_handlers: dict[str, list[TypedEventHandler]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._event_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._event_queue: asyncio.Queue[tuple[str, dict[str, Any]]] | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._connected_event = asyncio.Event()
        self._close_requested = False
        self._generation = 0
        self._desired_focused_app_subscription: dict[str, Any] | None = None
        self._desired_level_meter_subscriptions: dict[
            tuple[str, str, str | None], dict[str, Any]
        ] = {}
        self._plugin_devices: list[str] | None = None
        self.application_info: ApplicationInfo | None = None
        self._input_devices: list[InputDevice] = []
        self._output_devices: list[OutputDevice] = []
        self.main_output: MainOutput | None = None
        self._channels: list[Channel] = []
        self._mixes: list[Mix] = []
        self.level_meters: LevelMeterChanged | None = None
        self.focused_app: FocusedAppChanged | None = None
        self._logger = logging.getLogger(__name__)

    @property
    def input_devices(self) -> tuple[InputDevice, ...]:
        """Latest input-device state obtained by RPC or notifications."""
        return tuple(self._input_devices)

    @property
    def output_devices(self) -> tuple[OutputDevice, ...]:
        """Latest output-device state obtained by RPC or notifications."""
        return tuple(self._output_devices)

    @property
    def channels(self) -> tuple[Channel, ...]:
        """Latest channel state obtained by RPC or notifications."""
        return tuple(self._channels)

    @property
    def mixes(self) -> tuple[Mix, ...]:
        """Latest mix state obtained by RPC or notifications."""
        return tuple(self._mixes)

    # ------------------------------------------------------------------
    # Подключение и поиск порта
    # ------------------------------------------------------------------

    def discover_ports(self) -> list[int]:
        ports: list[int] = []

        for path in self._candidate_ws_info_paths():
            if not path.exists():
                continue

            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                raw_port = data["port"]
                if isinstance(raw_port, bool):
                    raise ValueError("boolean isn't a valid port")
                port = int(raw_port)
                if not 1 <= port <= 65535:
                    raise ValueError(f"port is outside 1..65535: {port}")
                if port not in ports:
                    ports.append(port)
            except (
                OSError,
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                if self.debug:
                    self._logger.debug(
                        "Ignoring invalid Wave Link port file %s: %s", path, exc
                    )
                continue

        for port in self.FALLBACK_PORTS:
            if port not in ports:
                ports.append(port)

        return ports

    def _candidate_ws_info_paths(self) -> list[Path]:
        result: list[Path] = []
        local_appdata_roots: list[Path] = []
        local_appdata = os.environ.get("LOCALAPPDATA")

        if local_appdata:
            local_appdata_roots.append(Path(local_appdata))

        # Wave Link работает в Windows, а адаптер часто запускается из WSL.
        # Переменные окружения Windows туда не наследуются, но состояние
        # пакета доступно через смонтированные диски Windows.
        try:
            for drive in Path("/mnt").glob("[a-z]"):
                local_appdata_roots.extend(drive.glob("Users/*/AppData/Local"))
        except OSError:
            pass

        deduped_roots: list[Path] = []
        seen_roots: set[str] = set()
        for root in local_appdata_roots:
            key = str(root).lower()
            if key not in seen_roots:
                seen_roots.add(key)
                deduped_roots.append(root)

        for root in deduped_roots:
            packages_dir = root / "Packages"
            result.append(
                packages_dir
                / "Elgato.WaveLink_g54w8ztgkx496"
                / "LocalState"
                / "ws-info.json"
            )

            try:
                for package in packages_dir.glob("Elgato.WaveLink*"):
                    result.append(package / "LocalState" / "ws-info.json")
            except OSError:
                pass

        deduped: list[Path] = []
        seen: set[str] = set()
        for path in result:
            key = str(path).lower()
            if key not in seen:
                seen.add(key)
                deduped.append(path)

        return deduped

    async def connect(self) -> None:
        self._close_requested = False
        await self._connect()

    async def _connect(self) -> None:
        async with self._lifecycle_lock:
            if self.state is ConnectionState.CONNECTED and self.ws is not None:
                return

            self._connected_event.clear()
            if self.state is not ConnectionState.DISCONNECTED or self.ws is not None:
                await self._reset_connection_locked()

            errors: list[tuple[int, Exception]] = []
            self.state = ConnectionState.CONNECTING

            try:
                for port in self.discover_ports():
                    url = f"ws://{self.host}:{port}"
                    try:
                        if self.debug:
                            self._logger.debug("Trying %s", url)

                        self._generation += 1
                        generation = self._generation
                        ws = await websockets.connect(
                            url,
                            origin=Origin("streamdeck://"),
                            ping_interval=20,
                            ping_timeout=20,
                            open_timeout=self.open_timeout,
                            close_timeout=self.close_timeout,
                            proxy=None,
                        )
                        self.ws = ws
                        self.connected_port = port
                        self._event_queue = asyncio.Queue(self.event_queue_size)
                        self._event_task = asyncio.create_task(
                            self._event_loop(generation),
                            name="WaveLinkEventDispatcher",
                        )
                        self._reader_task = asyncio.create_task(
                            self._reader_loop(ws, generation),
                            name="WaveLinkRpcReader",
                        )

                        app = await self._get_application_info(allow_connecting=True)
                        if app.app_id != "EWL":
                            raise WaveLinkProtocolError(
                                f"Connected to unexpected app: {app!r}"
                            )
                        try:
                            interface_revision = int(app.interface_revision)
                        except (TypeError, ValueError) as exc:
                            raise WaveLinkProtocolError(
                                "Wave Link did not report a valid interface revision: "
                                f"{app!r}"
                            ) from exc
                        if interface_revision < 1:
                            raise WaveLinkProtocolError(
                                "Unsupported Wave Link interface revision: "
                                f"{interface_revision}"
                            )
                        self.application_info = app

                        await self._restore_session()
                        if self.ws is not ws or generation != self._generation:
                            raise WaveLinkDisconnectedError(
                                "Wave Link disconnected while restoring the session"
                            )

                        self.state = ConnectionState.CONNECTED
                        self._connected_event.set()
                        if self.debug:
                            self._logger.debug("Connected to %s", url)
                        return
                    except asyncio.CancelledError:
                        await self._reset_connection_locked()
                        raise
                    except Exception as exc:
                        errors.append((port, exc))
                        if self.debug:
                            self._logger.debug("Failed %s: %s", url, exc)
                        await self._reset_connection_locked()
                        self.state = ConnectionState.CONNECTING
            finally:
                if self.state is ConnectionState.CONNECTING:
                    self.state = ConnectionState.DISCONNECTED

            summary = "; ".join(
                f"{port}: {type(exc).__name__}: {exc}" for port, exc in errors
            )
            raise ConnectionError(
                "Cannot connect to Elgato Wave Link WebSocket"
                + (f" ({summary})" if summary else "")
            )

    async def close(self) -> None:
        self._close_requested = True
        self._connected_event.clear()
        reconnect_task = self._reconnect_task
        self._reconnect_task = None
        if reconnect_task is not None and reconnect_task is not asyncio.current_task():
            reconnect_task.cancel()
            await asyncio.gather(reconnect_task, return_exceptions=True)

        async with self._lifecycle_lock:
            await self._reset_connection_locked()

    async def wait_until_connected(self, timeout: float | None = None) -> None:
        """Дождаться успешного первичного подключения или переподключения."""
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero or None")
        if self.state is ConnectionState.CONNECTED and self.ws is not None:
            return

        try:
            async with asyncio.timeout(timeout):
                await self._connected_event.wait()
        except TimeoutError as exc:
            raise WaveLinkTimeoutError(
                "Timed out waiting for the Wave Link connection"
            ) from exc

    async def _reset_connection(self) -> None:
        async with self._lifecycle_lock:
            await self._reset_connection_locked()

    async def _reset_connection_locked(self) -> None:
        self.state = ConnectionState.CLOSING
        self._connected_event.clear()
        self._generation += 1

        reader_task = self._reader_task
        self._reader_task = None
        event_task = self._event_task
        self._event_task = None
        self._event_queue = None
        ws = self.ws
        self.ws = None
        self.connected_port = None

        self._fail_pending(WaveLinkDisconnectedError("Wave Link connection closed"))

        current_task = asyncio.current_task()
        tasks = [
            task
            for task in (reader_task, event_task)
            if task is not None and task is not current_task
        ]
        for task in tasks:
            task.cancel()

        if ws is not None:
            try:
                await ws.close()
            except Exception as exc:
                if self.debug:
                    self._logger.debug("Error while closing Wave Link socket: %s", exc)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self.state = ConnectionState.DISCONNECTED

    def _fail_pending(self, exc: Exception) -> None:
        pending = tuple(self._pending.values())
        self._pending.clear()
        for request in pending:
            if not request.future.done():
                request.future.set_exception(exc)

    async def _mark_connection_lost(
        self,
        ws: WebSocketConnection,
        generation: int,
        exc: Exception,
    ) -> None:
        if generation != self._generation or self.ws is not ws:
            return

        self._generation += 1
        reader_task = self._reader_task
        event_task = self._event_task
        self.ws = None
        self.connected_port = None
        self._reader_task = None
        self._event_task = None
        self._event_queue = None
        self.state = ConnectionState.DISCONNECTED
        self._connected_event.clear()
        self._fail_pending(exc)

        current_task = asyncio.current_task()
        tasks = [
            task
            for task in (reader_task, event_task)
            if task is not None and task is not current_task
        ]
        for task in tasks:
            task.cancel()

        try:
            await ws.close()
        except Exception as close_exc:
            if self.debug:
                self._logger.debug(
                    "Error while closing lost Wave Link socket: %s", close_exc
                )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        if not self.auto_reconnect or self._close_requested:
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(
            self._reconnect_loop(), name="WaveLinkReconnect"
        )

    async def _reconnect_loop(self) -> None:
        delay = self.reconnect_initial_delay
        attempt = 0
        try:
            while self.auto_reconnect and not self._close_requested:
                attempt += 1
                try:
                    await self._connect()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._logger.warning(
                        "Wave Link reconnect attempt %d failed: %s; retrying in %.1fs",
                        attempt,
                        exc,
                        delay,
                    )
                else:
                    self._logger.info(
                        "Reconnected to Wave Link on %s:%s",
                        self.host,
                        self.connected_port,
                    )
                    return

                await asyncio.sleep(delay)
                delay = min(delay * self.reconnect_backoff, self.reconnect_max_delay)
        finally:
            if self._reconnect_task is asyncio.current_task():
                self._reconnect_task = None

    async def _restore_session(self) -> None:
        """Восстановить метаданные WebSocket и подписки после реконнекта."""
        operations: list[tuple[str, dict[str, Any]]] = []
        if self._plugin_devices is not None:
            operations.append(
                (
                    "setPluginInfo",
                    {"connectedDevices": list(self._plugin_devices)},
                )
            )
        if self._desired_focused_app_subscription is not None:
            operations.append(
                (
                    "setSubscription",
                    {
                        "focusedAppChanged": deepcopy(
                            self._desired_focused_app_subscription
                        )
                    },
                )
            )
        operations.extend(
            ("setSubscription", {"levelMeterChanged": deepcopy(subscription)})
            for subscription in self._desired_level_meter_subscriptions.values()
        )

        for method, params in operations:
            try:
                await self._call(method, params, allow_connecting=True)
            except WaveLinkDisconnectedError:
                raise
            except Exception as exc:
                self._logger.warning(
                    "Connected to Wave Link, but couldn't restore %s: %s",
                    method,
                    exc,
                )

    async def __aenter__(self) -> "WaveLinkClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Транспорт JSON-RPC
    # ------------------------------------------------------------------

    async def _reader_loop(self, ws: WebSocketConnection, generation: int) -> None:
        disconnect_error: Exception = WaveLinkDisconnectedError(
            "Wave Link closed the WebSocket connection"
        )
        try:
            async for raw in ws:
                if self.debug:
                    self._logger.debug("[ws <-] %s", raw)

                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                if not isinstance(msg, dict):
                    continue

                if "id" in msg:
                    request_id = msg["id"]
                    if type(request_id) is not int:
                        continue
                    request = self._pending.pop(request_id, None)
                    if (
                        request is None
                        or request.generation != generation
                        or request.future.done()
                    ):
                        continue

                    future = request.future
                    if "error" in msg:
                        error = msg["error"]
                        future.set_exception(
                            WaveLinkRpcError(
                                error if isinstance(error, dict) else str(error),
                                method=request.method,
                                request_id=request_id,
                            )
                        )
                    elif "result" in msg:
                        future.set_result(msg["result"])
                    else:
                        future.set_exception(
                            WaveLinkProtocolError(
                                f"RPC response {request_id} has neither result nor error"
                            )
                        )
                    continue

                method = msg.get("method")
                params = msg.get("params", {})
                if method:
                    queue = self._event_queue
                    if queue is None or generation != self._generation:
                        continue
                    event = (
                        str(method),
                        params if isinstance(params, dict) else {},
                    )
                    try:
                        queue.put_nowait(event)
                    except asyncio.QueueFull:
                        self._logger.warning(
                            "Dropping Wave Link event %s because the event queue is full",
                            method,
                        )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            disconnect_error = WaveLinkDisconnectedError(
                f"Wave Link WebSocket reader stopped: {exc}"
            )
            if self.debug:
                self._logger.debug("Reader loop stopped: %s", exc)
        finally:
            await self._mark_connection_lost(ws, generation, disconnect_error)

    async def _event_loop(self, generation: int) -> None:
        queue = self._event_queue
        if queue is None:
            return

        while generation == self._generation:
            method, params = await queue.get()
            try:
                await self._dispatch_event(method, params)
            finally:
                queue.task_done()

    async def _dispatch_event(self, method: str, params: dict[str, Any]) -> None:
        typed_event: WaveLinkEvent | None = None
        if method in self.TYPED_EVENT_METHODS:
            try:
                typed_event = self._parse_typed_event(method, params)
                typed_event = self._update_cached_event(method, typed_event)
            except (TypeError, WaveLinkProtocolError, WaveLinkSchemaError):
                self._logger.exception("Invalid Wave Link event payload for %s", method)

        if typed_event is not None:
            for handler in tuple(self._typed_event_handlers.get(method, ())):
                await self._invoke_event_handler(handler, typed_event, method)

        for handler in tuple(self._event_handlers.get(method, ())):
            await self._invoke_event_handler(handler, params, method)

    async def _invoke_event_handler(
        self,
        handler: EventHandler | TypedEventHandler,
        payload: dict[str, Any] | WaveLinkEvent,
        method: str,
    ) -> None:
        try:
            result = handler(payload)  # type: ignore[arg-type]
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            self._logger.exception(
                "Wave Link event handler cancelled itself for %s", method
            )
        except Exception:
            self._logger.exception("Wave Link event handler failed for %s", method)

    def _parse_typed_event(self, method: str, params: dict[str, Any]) -> WaveLinkEvent:
        if method == "inputDevicesChanged":
            return parse_schema_list(params, "inputDevices", InputDevice, method)
        if method == "inputDeviceChanged":
            return parse_schema(params, InputDevice, method)
        if method == "outputDevicesChanged":
            return parse_schema(params, OutputDevices, method)
        if method == "outputDeviceChanged":
            return parse_schema(params, OutputDevice, method)
        if method == "channelsChanged":
            return parse_schema_list(params, "channels", Channel, method)
        if method == "channelChanged":
            return parse_schema(params, Channel, method)
        if method == "mixesChanged":
            return parse_schema_list(params, "mixes", Mix, method)
        if method == "mixChanged":
            return parse_schema(params, Mix, method)
        if method == "levelMeterChanged":
            return parse_schema(params, LevelMeterChanged, method)
        if method == "focusedAppChanged":
            return parse_schema(params, FocusedAppChanged, method)
        if method == "createProfileRequested":
            return parse_schema(params, CreateProfileRequested, method)
        raise ValueError(f"Unsupported typed Wave Link event: {method}")

    def _update_cached_event(self, method: str, event: WaveLinkEvent) -> WaveLinkEvent:
        if method == "inputDevicesChanged":
            self._input_devices = list(event)  # type: ignore[arg-type]
        elif method == "inputDeviceChanged":
            assert isinstance(event, InputDevice)
            self._input_devices = _merge_identified_list(
                self._input_devices, event, _merge_input_device
            )
            return next(item for item in self._input_devices if item.id == event.id)
        elif method == "outputDevicesChanged":
            assert isinstance(event, OutputDevices)
            self.main_output = event.main_output
            self._output_devices = list(event.output_devices)
        elif method == "outputDeviceChanged":
            assert isinstance(event, OutputDevice)
            self._output_devices = _merge_identified_list(
                self._output_devices, event, _merge_output_device
            )
            return next(item for item in self._output_devices if item.id == event.id)
        elif method == "channelsChanged":
            self._channels = list(event)  # type: ignore[arg-type]
        elif method == "channelChanged":
            assert isinstance(event, Channel)
            self._channels = _merge_identified_list(self._channels, event, _merge_model)
            return next(item for item in self._channels if item.id == event.id)
        elif method == "mixesChanged":
            self._mixes = list(event)  # type: ignore[arg-type]
        elif method == "mixChanged":
            assert isinstance(event, Mix)
            self._mixes = _merge_identified_list(self._mixes, event, _merge_model)
            return next(item for item in self._mixes if item.id == event.id)
        elif method == "levelMeterChanged":
            assert isinstance(event, LevelMeterChanged)
            self.level_meters = event
        elif method == "focusedAppChanged":
            assert isinstance(event, FocusedAppChanged)
            self.focused_app = event
        return event

    def on(self, method: str, handler: EventHandler) -> None:
        """Зарегистрировать обработчик уведомлений указанного метода JSON-RPC."""
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._event_handlers.setdefault(method, []).append(handler)

    def off(self, method: str, handler: EventHandler) -> bool:
        """Удалить одну регистрацию обработчика уведомлений."""
        handlers = self._event_handlers.get(method)
        if not handlers:
            return False
        try:
            handlers.remove(handler)
        except ValueError:
            return False
        if not handlers:
            self._event_handlers.pop(method, None)
        return True

    def on_typed(self, method: str, handler: TypedEventHandler) -> None:
        """Register a validated object handler for a known Wave Link event."""
        if method not in self.TYPED_EVENT_METHODS:
            raise ValueError(f"Unsupported typed Wave Link event: {method}")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._typed_event_handlers.setdefault(method, []).append(handler)

    def off_typed(self, method: str, handler: TypedEventHandler) -> bool:
        """Remove one typed event-handler registration."""
        handlers = self._typed_event_handlers.get(method)
        if not handlers:
            return False
        try:
            handlers.remove(handler)
        except ValueError:
            return False
        if not handlers:
            self._typed_event_handlers.pop(method, None)
        return True

    async def stream_events(
        self,
        method: str,
        *,
        queue_size: int | None = None,
    ) -> AsyncIterator[WaveLinkEvent]:
        """Yield validated events without blocking the WebSocket event loop."""
        if method not in self.TYPED_EVENT_METHODS:
            raise ValueError(f"Unsupported typed Wave Link event: {method}")
        capacity = self.event_queue_size if queue_size is None else queue_size
        if capacity <= 0:
            raise ValueError("queue_size must be greater than zero")

        queue: asyncio.Queue[WaveLinkEvent] = asyncio.Queue(capacity)

        def enqueue(event: WaveLinkEvent) -> None:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                self._logger.warning(
                    "Dropping streamed Wave Link event %s because its queue is full",
                    method,
                )

        self.on_typed(method, enqueue)
        try:
            while True:
                event = await queue.get()
                queue.task_done()
                yield event
        finally:
            self.off_typed(method, enqueue)

    async def stream_level_meters(
        self, *, queue_size: int | None = None
    ) -> AsyncIterator[LevelMeterChanged]:
        stream = self.stream_events("levelMeterChanged", queue_size=queue_size)
        try:
            async for event in stream:
                assert isinstance(event, LevelMeterChanged)
                yield event
        finally:
            await stream.aclose()

    async def stream_focused_app_changes(
        self, *, queue_size: int | None = None
    ) -> AsyncIterator[FocusedAppChanged]:
        stream = self.stream_events("focusedAppChanged", queue_size=queue_size)
        try:
            async for event in stream:
                assert isinstance(event, FocusedAppChanged)
                yield event
        finally:
            await stream.aclose()

    async def stream_input_device_changes(
        self, *, queue_size: int | None = None
    ) -> AsyncIterator[InputDevice]:
        stream = self.stream_events("inputDeviceChanged", queue_size=queue_size)
        try:
            async for event in stream:
                assert isinstance(event, InputDevice)
                yield event
        finally:
            await stream.aclose()

    async def call(
        self,
        method: str,
        params: Any = None,
        *,
        timeout: float | None = None,
    ) -> JsonValue:
        """Отправить JSON-RPC-вызов и дождаться соответствующего ответа."""
        return await self._call(method, params, timeout=timeout)

    async def _call(
        self,
        method: str,
        params: Any,
        *,
        timeout: float | None = None,
        allow_connecting: bool = False,
    ) -> JsonValue:
        if not isinstance(method, str) or not method:
            raise ValueError("method must be a non-empty string")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero or None")

        effective_timeout = self.rpc_timeout if timeout is None else timeout
        reconnect_task = self._reconnect_task
        if (
            not allow_connecting
            and (self.state is not ConnectionState.CONNECTED or self.ws is None)
            and reconnect_task is not None
            and not reconnect_task.done()
            and not self._close_requested
        ):
            await self.wait_until_connected(effective_timeout)

        ws = self.ws
        reader_task = self._reader_task
        allowed_states = (
            {ConnectionState.CONNECTING, ConnectionState.CONNECTED}
            if allow_connecting
            else {ConnectionState.CONNECTED}
        )
        if (
            ws is None
            or self.state not in allowed_states
            or reader_task is None
            or reader_task.done()
        ):
            raise WaveLinkDisconnectedError("Wave Link is not connected")

        request_id = self._next_id
        self._next_id += 1

        future: asyncio.Future[JsonValue] = asyncio.get_running_loop().create_future()
        request = _PendingRequest(future, method, self._generation)
        self._pending[request_id] = request
        payload = {
            "id": request_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
        raw = json.dumps(payload, ensure_ascii=False)

        if self.debug:
            self._logger.debug("[ws ->] %s", raw)

        try:
            try:
                await ws.send(raw)
            except Exception as exc:
                disconnect_error = WaveLinkDisconnectedError(
                    f"Failed to send Wave Link RPC {method}: {exc}"
                )
                self._pending.pop(request_id, None)
                future.cancel()
                await self._mark_connection_lost(
                    ws, request.generation, disconnect_error
                )
                raise disconnect_error from exc

            try:
                async with asyncio.timeout(effective_timeout):
                    return await future
            except TimeoutError as exc:
                raise WaveLinkTimeoutError(
                    f"Wave Link RPC {method} timed out after "
                    f"{effective_timeout:g} seconds"
                ) from exc
        finally:
            current = self._pending.get(request_id)
            if current is request:
                self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    # ------------------------------------------------------------------
    # Обёртки API Wave Link
    # ------------------------------------------------------------------

    async def get_application_info(self) -> ApplicationInfo:
        return await self._get_application_info(allow_connecting=False)

    async def _get_application_info(self, *, allow_connecting: bool) -> ApplicationInfo:
        result = await self._call(
            "getApplicationInfo", None, allow_connecting=allow_connecting
        )
        info = parse_schema(result, ApplicationInfo, "getApplicationInfo")
        self.application_info = info
        return info

    async def get_channels(self) -> list[Channel]:
        result = await self.call("getChannels", None)
        channels = parse_schema_list(result, "channels", Channel, "getChannels")
        self._channels = channels
        return channels

    async def get_mixes(self) -> list[Mix]:
        result = await self.call("getMixes", None)
        mixes = parse_schema_list(result, "mixes", Mix, "getMixes")
        self._mixes = mixes
        return mixes

    async def get_input_devices(self) -> list[InputDevice]:
        result = await self.call("getInputDevices", None)
        input_devices = parse_schema_list(
            result, "inputDevices", InputDevice, "getInputDevices"
        )
        self._input_devices = input_devices
        return input_devices

    async def get_output_devices(self) -> OutputDevices:
        result = await self.call("getOutputDevices", None)
        output_devices = parse_schema(result, OutputDevices, "getOutputDevices")
        self.main_output = output_devices.main_output
        self._output_devices = output_devices.output_devices
        return output_devices

    async def set_plugin_info(self, connected_devices: list[str]) -> PluginInfoResult:
        """Сообщить Wave Link семейства подключённых устройств Stream Deck."""
        devices = [str(device) for device in connected_devices]
        result = await self.call(
            "setPluginInfo",
            {"connectedDevices": devices},
        )
        self._plugin_devices = devices
        return parse_schema(result, PluginInfoResult, "setPluginInfo")

    async def set_input_device(
        self,
        device_id: str,
        inputs: list[InputUpdate],
    ) -> InputDeviceUpdate:
        """Обновить один или несколько входов указанного входного устройства."""
        for index, item in enumerate(inputs):
            if not isinstance(item, InputUpdate):
                raise TypeError(f"inputs[{index}] must be InputUpdate")
        payload = InputDeviceUpdate(id=str(device_id), inputs=list(inputs))
        result = await self.call(
            "setInputDevice",
            payload.to_dict(),
        )
        return parse_schema(result, InputDeviceUpdate, "setInputDevice")

    async def set_input_mute(
        self, device_id: str, input_id: str, muted: bool
    ) -> InputDeviceUpdate:
        return await self.set_input_device(
            device_id,
            [InputUpdate(id=str(input_id), is_muted=bool(muted))],
        )

    async def set_input_gain(
        self, device_id: str, input_id: str, value: float
    ) -> InputDeviceUpdate:
        return await self.set_input_device(
            device_id,
            [InputUpdate(id=str(input_id), gain=LevelValue(clamp01(value)))],
        )

    async def set_input_gain_lock(
        self, device_id: str, input_id: str, enabled: bool
    ) -> InputDeviceUpdate:
        """Enable or disable the hardware gain lock for an input."""
        return await self.set_input_device(
            device_id,
            [
                InputUpdate(
                    id=str(input_id),
                    is_gain_lock_on=bool(enabled),
                )
            ],
        )

    async def set_input_mic_pc_mix(
        self, device_id: str, input_id: str, value: float
    ) -> InputDeviceUpdate:
        return await self.set_input_device(
            device_id,
            [
                InputUpdate(
                    id=str(input_id),
                    mic_pc_mix=LevelValue(clamp01(value)),
                )
            ],
        )

    async def set_input_effect_enabled(
        self,
        device_id: str,
        input_id: str,
        effect_id: str,
        enabled: bool,
        *,
        dsp: bool = False,
    ) -> InputDeviceUpdate:
        """Включить программный либо аппаратный/DSP-эффект входа."""
        effect = EffectUpdate(id=str(effect_id), is_enabled=bool(enabled))
        update = InputUpdate(id=str(input_id))
        if dsp:
            update.dsp_effects = [effect]
        else:
            update.effects = [effect]
        return await self.set_input_device(device_id, [update])

    async def set_output_device(
        self, params: OutputDeviceUpdateParams
    ) -> OutputDeviceUpdateResult:
        """Отправить документированную структуру параметров ``setOutputDevice``."""
        if not isinstance(
            params, (SetOutputDeviceParams, MainOutput, OutputDeviceUpdate)
        ):
            raise TypeError("params must be an output-device update schema")
        result = await self.call("setOutputDevice", params.to_dict())
        return parse_output_device_result(result)

    async def set_main_output(
        self, output_device_id: str, output_id: str = ""
    ) -> OutputDeviceUpdateResult:
        main_output = MainOutput(
            output_device_id=str(output_device_id),
            output_id=str(output_id),
        )
        payload = SetOutputDeviceParams(main_output=main_output)
        try:
            return await self.set_output_device(payload)
        except WaveLinkRpcError as exc:
            if not exc.is_invalid_params:
                raise
            # Совместимость со сборками, которые принимали поля главного
            # выхода напрямую, а не внутри документированного ``mainOutput``.
            return await self.set_output_device(main_output)

    async def _set_output_value(
        self,
        output_device_id: str,
        output_id: str,
        *,
        level: float | None = None,
        muted: bool | None = None,
        mix_id: str | None = None,
    ) -> OutputDeviceUpdateResult:
        output = OutputUpdate(id=str(output_id))
        if level is not None:
            output.level = clamp01(level)
        if muted is not None:
            output.is_muted = bool(muted)
        if mix_id is not None:
            output.mix_id = str(mix_id)

        output_device = OutputDeviceUpdate(
            id=str(output_device_id),
            outputs=[output],
        )
        documented = SetOutputDeviceParams(output_device=output_device)
        try:
            return await self.set_output_device(documented)
        except WaveLinkRpcError as exc:
            if not exc.is_invalid_params:
                raise
            # Совместимость с экспериментальными сборками с плоской структурой.
            return await self.set_output_device(output_device)

    async def set_output_level(
        self, output_device_id: str, output_id: str, level: float
    ) -> OutputDeviceUpdateResult:
        return await self._set_output_value(output_device_id, output_id, level=level)

    async def set_output_mute(
        self, output_device_id: str, output_id: str, muted: bool
    ) -> OutputDeviceUpdateResult:
        return await self._set_output_value(output_device_id, output_id, muted=muted)

    async def set_output_mix(
        self, output_device_id: str, output_id: str, mix_id: str
    ) -> OutputDeviceUpdateResult:
        return await self._set_output_value(output_device_id, output_id, mix_id=mix_id)

    async def remove_output_from_mix(
        self, output_device_id: str, output_id: str
    ) -> OutputDeviceUpdateResult:
        return await self.set_output_mix(output_device_id, output_id, "")

    async def set_channel(self, params: ChannelUpdate) -> ChannelUpdate:
        """Обновить любой поддерживаемый набор свойств канала."""
        if not isinstance(params, ChannelUpdate):
            raise TypeError("params must be ChannelUpdate")
        result = await self.call("setChannel", params.to_dict())
        return parse_schema(result, ChannelUpdate, "setChannel")

    async def set_channel_level(self, channel_id: str, level: float) -> ChannelUpdate:
        return await self.set_channel(
            ChannelUpdate(id=str(channel_id), level=clamp01(level))
        )

    async def set_channel_mute(self, channel_id: str, muted: bool) -> ChannelUpdate:
        return await self.set_channel(
            ChannelUpdate(id=str(channel_id), is_muted=bool(muted))
        )

    async def set_channel_mix_level(
        self, channel_id: str, mix_id: str, level: float
    ) -> ChannelUpdate:
        """Задать уровень канала в миксе с поддержкой обеих известных форм ID."""
        try:
            return await self.set_channel(
                ChannelUpdate(
                    id=str(channel_id),
                    mixes=[ChannelMixUpdate(id=str(mix_id), level=clamp01(level))],
                )
            )
        except WaveLinkRpcError as exc:
            if not exc.is_invalid_params:
                raise
            return await self.set_channel(
                ChannelUpdate(
                    id=str(channel_id),
                    mixes=[ChannelMixUpdate(mix_id=str(mix_id), level=clamp01(level))],
                )
            )

    async def set_channel_mix_mute(
        self, channel_id: str, mix_id: str, muted: bool
    ) -> ChannelUpdate:
        """Заглушить канал в миксе с поддержкой обеих известных форм ID."""
        try:
            return await self.set_channel(
                ChannelUpdate(
                    id=str(channel_id),
                    mixes=[ChannelMixUpdate(id=str(mix_id), is_muted=bool(muted))],
                )
            )
        except WaveLinkRpcError as exc:
            if not exc.is_invalid_params:
                raise
            return await self.set_channel(
                ChannelUpdate(
                    id=str(channel_id),
                    mixes=[ChannelMixUpdate(mix_id=str(mix_id), is_muted=bool(muted))],
                )
            )

    async def set_channel_effect_enabled(
        self, channel_id: str, effect_id: str, enabled: bool
    ) -> ChannelUpdate:
        return await self.set_channel(
            ChannelUpdate(
                id=str(channel_id),
                effects=[EffectUpdate(id=str(effect_id), is_enabled=bool(enabled))],
            )
        )

    async def set_mix(self, params: MixUpdate) -> MixUpdate:
        """Обновить любой поддерживаемый набор свойств микса."""
        if not isinstance(params, MixUpdate):
            raise TypeError("params must be MixUpdate")
        result = await self.call("setMix", params.to_dict())
        return parse_schema(result, MixUpdate, "setMix")

    async def set_mix_level(self, mix_id: str, level: float) -> MixUpdate:
        return await self.set_mix(MixUpdate(id=str(mix_id), level=clamp01(level)))

    async def set_mix_mute(self, mix_id: str, muted: bool) -> MixUpdate:
        return await self.set_mix(MixUpdate(id=str(mix_id), is_muted=bool(muted)))

    async def add_to_channel(self, app_id: str, channel_id: str) -> Channel:
        """Назначить приложение программному каналу."""
        result = await self.call(
            "addToChannel",
            {"appId": str(app_id), "channelId": str(channel_id)},
        )
        return parse_schema(result, Channel, "addToChannel")

    async def set_subscription(self, params: SubscriptionUpdate) -> SubscriptionUpdate:
        """Обновить одну или несколько подписок на уведомления Wave Link."""
        if not isinstance(params, SubscriptionUpdate):
            raise TypeError("params must be SubscriptionUpdate")
        payload = params.to_dict()
        result = await self.call("setSubscription", payload)
        update = parse_schema(result, SubscriptionUpdate, "setSubscription")
        response_payload = update.to_dict()
        for key in payload:
            if key not in response_payload:
                raise WaveLinkProtocolError(
                    f"setSubscription returned a missing {key!r} field"
                )
        self._remember_subscription(payload)
        return update

    def _remember_subscription(self, payload: dict[str, JsonValue]) -> None:
        focused = payload.get("focusedAppChanged")
        if isinstance(focused, dict):
            if focused.get("isEnabled") is True:
                self._desired_focused_app_subscription = deepcopy(focused)
            else:
                self._desired_focused_app_subscription = None

        meter = payload.get("levelMeterChanged")
        if not isinstance(meter, dict):
            return
        meter_type = meter.get("type")
        target_id = meter.get("id")
        raw_sub_id = meter.get("subId")
        if not isinstance(meter_type, str) or not isinstance(target_id, str):
            return
        sub_id = raw_sub_id if isinstance(raw_sub_id, str) else None
        key = (meter_type, target_id, sub_id)
        if meter.get("isEnabled") is True:
            self._desired_level_meter_subscriptions[key] = deepcopy(meter)
            return

        if sub_id is not None:
            self._desired_level_meter_subscriptions.pop(key, None)
            return
        for existing_key in tuple(self._desired_level_meter_subscriptions):
            if existing_key[:2] == (meter_type, target_id):
                self._desired_level_meter_subscriptions.pop(existing_key, None)

    async def subscribe_focused_app(self, enabled: bool = True) -> SubscriptionUpdate:
        return await self.set_subscription(
            SubscriptionUpdate(
                focused_app_changed=FocusedAppSubscription(bool(enabled))
            )
        )

    async def subscribe_level_meter(
        self,
        meter_type: LevelMeterType,
        target_id: str,
        enabled: bool = True,
        *,
        sub_id: str | None = None,
    ) -> SubscriptionUpdate:
        if meter_type not in self.LEVEL_METER_TYPES:
            allowed = ", ".join(self.LEVEL_METER_TYPES)
            raise ValueError(
                f"Unsupported level-meter type {meter_type!r}; expected {allowed}"
            )

        subscription = LevelMeterSubscription(
            type=meter_type,
            id=str(target_id),
            is_enabled=bool(enabled),
            sub_id=str(sub_id) if sub_id is not None else None,
        )
        return await self.set_subscription(
            SubscriptionUpdate(level_meter_changed=subscription)
        )

    async def subscribe_realtime(self) -> SubscriptionUpdate:
        """Включить подписку, работоспособность которой проверена в Wave Link 3.2.5."""
        return await self.subscribe_focused_app(True)

    async def try_subscribe_level_meters(
        self,
    ) -> dict[LevelMeterType, list[SubscriptionUpdate]]:
        """Subscribe to every currently available meter by its concrete ID."""
        await self.subscribe_focused_app(True)
        input_devices, output_state, channels, mixes = await asyncio.gather(
            self.get_input_devices(),
            self.get_output_devices(),
            self.get_channels(),
            self.get_mixes(),
        )
        targets: dict[LevelMeterType, list[str]] = {
            "input": [
                input_.id for device in input_devices for input_ in device.inputs or []
            ],
            "output": [
                output.id
                for device in output_state.output_devices
                for output in device.outputs or []
            ],
            "channel": [channel.id for channel in channels],
            "mix": [mix.id for mix in mixes],
        }
        results: dict[LevelMeterType, list[SubscriptionUpdate]] = {}
        for meter_type, target_ids in targets.items():
            results[meter_type] = []
            for target_id in dict.fromkeys(target_ids):
                results[meter_type].append(
                    await self.subscribe_level_meter(meter_type, target_id, True)
                )
        return results


ModelT = TypeVar("ModelT", bound=JsonModel)


def _merge_model(
    existing: ModelT,
    update: ModelT,
    *,
    ignored_fields: frozenset[str] = frozenset(),
) -> ModelT:
    """Merge a partial notification model without mutating cached instances."""
    merged = deepcopy(existing)
    for model_field in fields(update):
        name = model_field.name
        if name in ignored_fields:
            continue
        value = getattr(update, name)
        if name == "extra":
            merged.extra.update(deepcopy(value))
        elif value is not None:
            setattr(merged, name, deepcopy(value))
    return merged


def _merge_identified_list(
    current: list[ModelT],
    update: ModelT,
    merger: Callable[[ModelT, ModelT], ModelT],
) -> list[ModelT]:
    """Return a new list with an identified partial update merged into it."""
    identifier = getattr(update, "id")
    result = list(current)
    for index, existing in enumerate(result):
        if getattr(existing, "id") == identifier:
            result[index] = merger(existing, update)
            return result
    result.append(deepcopy(update))
    return result


def _merge_input_device(existing: InputDevice, update: InputDevice) -> InputDevice:
    merged = _merge_model(existing, update, ignored_fields=frozenset({"inputs"}))
    if update.inputs is not None:
        inputs = list(existing.inputs or [])
        for input_update in update.inputs:
            inputs = _merge_identified_list(inputs, input_update, _merge_model)
        merged.inputs = inputs
    return merged


def _merge_output_device(existing: OutputDevice, update: OutputDevice) -> OutputDevice:
    merged = _merge_model(existing, update, ignored_fields=frozenset({"outputs"}))
    if update.outputs is not None:
        outputs = list(existing.outputs or [])
        for output_update in update.outputs:
            outputs = _merge_identified_list(outputs, output_update, _merge_model)
        merged.outputs = outputs
    return merged


def expect_object(result: Any, method: str) -> dict[str, Any]:
    """Проверить результат JSON-RPC, который должен быть объектом."""
    if not isinstance(result, dict):
        raise WaveLinkProtocolError(
            f"{method} returned {type(result).__name__}; expected an object"
        )
    return result


SchemaT = TypeVar("SchemaT", bound=JsonModel)


def parse_schema(result: Any, schema: type[SchemaT], method: str) -> SchemaT:
    """Convert a JSON-RPC object response into a validated object schema."""
    value = expect_object(result, method)
    try:
        return schema.from_dict(value)
    except WaveLinkSchemaError as exc:
        raise WaveLinkProtocolError(f"{method} returned invalid data: {exc}") from exc


def parse_schema_list(
    result: Any,
    key: str,
    schema: type[SchemaT],
    method: str,
) -> list[SchemaT]:
    """Convert an object property containing a list of schema objects."""
    container = expect_object(result, method)
    items = container.get(key)
    if not isinstance(items, list):
        raise WaveLinkProtocolError(f"{method} returned an invalid {key!r} collection")

    converted: list[SchemaT] = []
    for index, item in enumerate(items):
        try:
            converted.append(schema.from_dict(item, path=f"{key}[{index}]"))
        except (TypeError, WaveLinkSchemaError) as exc:
            raise WaveLinkProtocolError(
                f"{method} returned invalid data: {exc}"
            ) from exc
    return converted


def parse_output_device_result(result: Any) -> OutputDeviceUpdateResult:
    """Parse all known result shapes returned by ``setOutputDevice``."""
    value = expect_object(result, "setOutputDevice")
    try:
        if "mainOutput" in value or "outputDevice" in value:
            return SetOutputDeviceParams.from_dict(value)
        if "outputDeviceId" in value or "outputId" in value:
            return MainOutput.from_dict(value)
        return OutputDeviceUpdate.from_dict(value)
    except WaveLinkSchemaError as exc:
        raise WaveLinkProtocolError(
            f"setOutputDevice returned invalid data: {exc}"
        ) from exc


def clamp01(value: float) -> float:
    """Ограничить числовой уровень Wave Link поддерживаемым диапазоном 0..1."""
    if isinstance(value, bool):
        raise TypeError("Wave Link level must be numeric, not bool")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Wave Link level must be finite")
    return max(0.0, min(1.0, number))


__all__ = [
    "ConnectionState",
    "EventHandler",
    "TypedEventHandler",
    "WaveLinkClient",
    "WaveLinkDisconnectedError",
    "WaveLinkProtocolError",
    "WaveLinkRpcError",
    "WaveLinkTimeoutError",
    "WaveLinkEvent",
    "clamp01",
]
