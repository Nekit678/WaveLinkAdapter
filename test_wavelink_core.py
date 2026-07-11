from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from wavelink_core import (
    ConnectionState,
    WaveLinkClient,
    WaveLinkDisconnectedError,
    WaveLinkProtocolError,
    WaveLinkRpcError,
    WaveLinkTimeoutError,
    clamp01,
)


_CLOSE = object()


class FakeWaveLinkSocket:
    def __init__(
        self,
        *,
        ignored_methods: set[str] | None = None,
        failing_methods: set[str] | None = None,
    ) -> None:
        self.incoming: asyncio.Queue[str | object] = asyncio.Queue()
        self.sent: list[dict[str, object]] = []
        self.ignored_methods = ignored_methods or set()
        self.failing_methods = failing_methods or set()
        self.closed = False

    def __aiter__(self) -> "FakeWaveLinkSocket":
        return self

    async def __anext__(self) -> str:
        message = await self.incoming.get()
        if message is _CLOSE:
            raise StopAsyncIteration
        assert isinstance(message, str)
        return message

    async def send(self, raw: str) -> None:
        payload = json.loads(raw)
        self.sent.append(payload)
        method = payload["method"]
        if method in self.failing_methods:
            raise ConnectionError("socket write failed")
        if method in self.ignored_methods:
            return

        if method == "getApplicationInfo":
            result: object = {"appID": "EWL", "interfaceRevision": 1}
        elif method == "setPluginInfo":
            result = {}
        elif method in {
            "setInputDevice",
            "setOutputDevice",
            "setChannel",
            "setMix",
            "setSubscription",
        }:
            result = payload["params"]
        elif method == "addToChannel":
            result = {"id": payload["params"]["channelId"]}
        elif method == "invalidParams":
            await self.respond_error(
                payload["id"], -32602, "bad params", {"field": "mixes"}
            )
            return
        else:
            result = {"method": method}
        await self.respond(payload["id"], result)

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.incoming.put(_CLOSE)

    async def remote_close(self) -> None:
        await self.incoming.put(_CLOSE)

    async def notify(self, method: str, params: dict[str, object]) -> None:
        await self.incoming.put(
            json.dumps({"jsonrpc": "2.0", "method": method, "params": params})
        )

    async def respond(self, request_id: object, result: object) -> None:
        await self.incoming.put(
            json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result})
        )

    async def respond_error(
        self, request_id: object, code: int, message: str, data: object
    ) -> None:
        await self.incoming.put(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": code, "message": message, "data": data},
                }
            )
        )


class WaveLinkRpcWrapperTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = WaveLinkClient()

        async def echo_update(method: str, params: object) -> object:
            if method == "setPluginInfo":
                return {}
            if method == "addToChannel":
                assert isinstance(params, dict)
                return {"id": params["channelId"]}
            return params

        self.client.call = AsyncMock(  # type: ignore[method-assign]
            side_effect=echo_update
        )

    def test_fallback_ports_only_target_the_wave_link_3_protocol(self) -> None:
        self.assertEqual(WaveLinkClient.FALLBACK_PORTS, list(range(1884, 1894)))

    async def test_missing_top_level_methods_use_documented_payloads(self) -> None:
        await self.client.set_plugin_info(["SD", "SDPlus"])
        await self.client.set_input_device("device", [{"id": "input", "isMuted": True}])
        await self.client.set_output_device(
            {"mainOutput": {"outputDeviceId": "device", "outputId": "output"}}
        )
        await self.client.add_to_channel("app", "channel")

        self.client.call.assert_has_awaits(
            [
                call("setPluginInfo", {"connectedDevices": ["SD", "SDPlus"]}),
                call(
                    "setInputDevice",
                    {"id": "device", "inputs": [{"id": "input", "isMuted": True}]},
                ),
                call(
                    "setOutputDevice",
                    {
                        "mainOutput": {
                            "outputDeviceId": "device",
                            "outputId": "output",
                        }
                    },
                ),
                call("addToChannel", {"appId": "app", "channelId": "channel"}),
            ]
        )

    async def test_input_convenience_methods_clamp_and_select_effect_collection(
        self,
    ) -> None:
        await self.client.set_input_gain("device", "input", 1.5)
        await self.client.set_input_mic_pc_mix("device", "input", -1)
        await self.client.set_input_effect_enabled(
            "device", "input", "clipguard", False, dsp=True
        )

        self.client.call.assert_has_awaits(
            [
                call(
                    "setInputDevice",
                    {
                        "id": "device",
                        "inputs": [{"id": "input", "gain": {"value": 1.0}}],
                    },
                ),
                call(
                    "setInputDevice",
                    {
                        "id": "device",
                        "inputs": [{"id": "input", "micPcMix": {"value": 0.0}}],
                    },
                ),
                call(
                    "setInputDevice",
                    {
                        "id": "device",
                        "inputs": [
                            {
                                "id": "input",
                                "dspEffects": [{"id": "clipguard", "isEnabled": False}],
                            }
                        ],
                    },
                ),
            ]
        )

    async def test_output_helpers_use_documented_nested_shape(self) -> None:
        await self.client.set_output_level("device", "output", 0.4)
        await self.client.set_output_mute("device", "output", True)
        await self.client.set_output_mix("device", "output", "monitor")
        await self.client.remove_output_from_mix("device", "output")

        payloads = [
            await_call.args[1] for await_call in self.client.call.await_args_list
        ]
        self.assertEqual(
            payloads,
            [
                {
                    "outputDevice": {
                        "id": "device",
                        "outputs": [{"id": "output", "level": 0.4}],
                    }
                },
                {
                    "outputDevice": {
                        "id": "device",
                        "outputs": [{"id": "output", "isMuted": True}],
                    }
                },
                {
                    "outputDevice": {
                        "id": "device",
                        "outputs": [{"id": "output", "mixId": "monitor"}],
                    }
                },
                {
                    "outputDevice": {
                        "id": "device",
                        "outputs": [{"id": "output", "mixId": ""}],
                    }
                },
            ],
        )

    async def test_channel_mix_prefers_current_id_shape(self) -> None:
        await self.client.set_channel_mix_level("channel", "mix", 0.75)

        self.client.call.assert_awaited_once_with(
            "setChannel",
            {"id": "channel", "mixes": [{"id": "mix", "level": 0.75}]},
        )

    async def test_channel_mix_retries_legacy_shape_only_for_rpc_error(self) -> None:
        self.client.call.side_effect = [
            WaveLinkRpcError("bad shape", code=-32602),
            {"id": "channel", "mixes": [{"mixId": "mix", "isMuted": True}]},
        ]

        await self.client.set_channel_mix_mute("channel", "mix", True)

        self.client.call.assert_has_awaits(
            [
                call(
                    "setChannel",
                    {
                        "id": "channel",
                        "mixes": [{"id": "mix", "isMuted": True}],
                    },
                ),
                call(
                    "setChannel",
                    {
                        "id": "channel",
                        "mixes": [{"mixId": "mix", "isMuted": True}],
                    },
                ),
            ]
        )

    async def test_level_meter_subscription_is_validated_and_parameterized(
        self,
    ) -> None:
        await self.client.subscribe_level_meter(
            "output", "device", False, sub_id="headphones"
        )

        self.client.call.assert_awaited_once_with(
            "setSubscription",
            {
                "levelMeterChanged": {
                    "type": "output",
                    "id": "device",
                    "isEnabled": False,
                    "subId": "headphones",
                }
            },
        )

        with self.assertRaises(ValueError):
            await self.client.subscribe_level_meter("all", "all")

    async def test_channel_mix_does_not_retry_unrelated_rpc_errors(self) -> None:
        error = WaveLinkRpcError("device not found", code=-32004)
        self.client.call.side_effect = error

        with self.assertRaises(WaveLinkRpcError) as raised:
            await self.client.set_channel_mix_level("channel", "mix", 0.5)

        self.assertIs(raised.exception, error)
        self.client.call.assert_awaited_once()


class WaveLinkTransportTests(unittest.IsolatedAsyncioTestCase):
    async def connect_fake(
        self,
        socket: FakeWaveLinkSocket | None = None,
        **client_kwargs: object,
    ) -> tuple[WaveLinkClient, FakeWaveLinkSocket, AsyncMock]:
        fake_socket = socket or FakeWaveLinkSocket()
        client_kwargs.setdefault("auto_reconnect", False)
        client = WaveLinkClient(**client_kwargs)
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(return_value=fake_socket)
        patcher = patch("wavelink_core.websockets.connect", connector)
        patcher.start()
        self.addCleanup(patcher.stop)
        await client.connect()
        self.addAsyncCleanup(client.close)
        return client, fake_socket, connector

    async def test_clean_remote_close_marks_client_disconnected(self) -> None:
        client, socket, _ = await self.connect_fake()
        reader = client._reader_task
        assert reader is not None

        await socket.remote_close()
        await asyncio.wait_for(reader, timeout=0.2)

        self.assertIs(client.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(client.ws)
        self.assertIsNone(client.connected_port)
        with self.assertRaises(WaveLinkDisconnectedError):
            await client.call("afterClose")

    async def test_event_handler_can_make_rpc_call(self) -> None:
        client, socket, _ = await self.connect_fake()
        handled = asyncio.Event()
        result: dict[str, object] = {}

        async def handler(params: dict[str, object]) -> None:
            result.update(await client.call("fromHandler"))
            handled.set()

        client.on("changed", handler)  # type: ignore[arg-type]
        await socket.notify("changed", {"value": 1})

        await asyncio.wait_for(handled.wait(), timeout=0.2)
        self.assertEqual(result, {"method": "fromHandler"})
        self.assertIs(client.state, ConnectionState.CONNECTED)

    async def test_event_handler_can_close_client(self) -> None:
        client, socket, _ = await self.connect_fake()
        closed = asyncio.Event()

        async def handler(params: dict[str, object]) -> None:
            await client.close()
            closed.set()

        client.on("shutdown", handler)  # type: ignore[arg-type]
        await socket.notify("shutdown", {})

        await asyncio.wait_for(closed.wait(), timeout=0.2)
        self.assertIs(client.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(client.ws)

    async def test_handler_failure_is_isolated(self) -> None:
        client, socket, _ = await self.connect_fake()
        handled = asyncio.Event()

        def broken_handler(params: dict[str, object]) -> None:
            raise RuntimeError("handler failed")

        def healthy_handler(params: dict[str, object]) -> None:
            handled.set()

        client.on("changed", broken_handler)  # type: ignore[arg-type]
        client.on("changed", healthy_handler)  # type: ignore[arg-type]
        with self.assertLogs("wavelink_core", level="ERROR"):
            await socket.notify("changed", {})
            await asyncio.wait_for(handled.wait(), timeout=0.2)

        self.assertEqual(await client.call("stillAlive"), {"method": "stillAlive"})

    async def test_rpc_timeout_cleans_pending_request(self) -> None:
        socket = FakeWaveLinkSocket(ignored_methods={"neverAnswers"})
        client, _, _ = await self.connect_fake(socket, rpc_timeout=0.01)

        with self.assertRaises(WaveLinkTimeoutError):
            await client.call("neverAnswers")

        self.assertEqual(client._pending, {})

    async def test_send_failure_disconnects_client(self) -> None:
        socket = FakeWaveLinkSocket(failing_methods={"cannotSend"})
        client, _, _ = await self.connect_fake(socket)

        with self.assertRaises(WaveLinkDisconnectedError):
            await client.call("cannotSend")

        self.assertIs(client.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(client.ws)
        self.assertEqual(client._pending, {})

    async def test_cancelled_rpc_cleans_pending_request(self) -> None:
        socket = FakeWaveLinkSocket(ignored_methods={"cancelMe"})
        client, _, _ = await self.connect_fake(socket)
        task = asyncio.create_task(client.call("cancelMe"))
        await asyncio.sleep(0)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(client._pending, {})

    async def test_rpc_errors_preserve_json_rpc_fields(self) -> None:
        client, _, _ = await self.connect_fake()

        with self.assertRaises(WaveLinkRpcError) as raised:
            await client.call("invalidParams")

        error = raised.exception
        self.assertEqual(error.code, -32602)
        self.assertEqual(error.message, "bad params")
        self.assertEqual(error.data, {"field": "mixes"})
        self.assertEqual(error.method, "invalidParams")
        self.assertTrue(error.is_invalid_params)

    async def test_concurrent_calls_match_out_of_order_responses(self) -> None:
        socket = FakeWaveLinkSocket(ignored_methods={"first", "second"})
        client, _, _ = await self.connect_fake(socket)
        first = asyncio.create_task(client.call("first"))
        second = asyncio.create_task(client.call("second"))

        while len(socket.sent) < 3:
            await asyncio.sleep(0)
        first_request, second_request = socket.sent[-2:]
        await socket.respond(second_request["id"], {"order": 2})
        await socket.respond(first_request["id"], {"order": 1})

        self.assertEqual(await first, {"order": 1})
        self.assertEqual(await second, {"order": 2})

    async def test_connect_is_idempotent(self) -> None:
        client, _, connector = await self.connect_fake()

        await client.connect()

        connector.assert_awaited_once()

    async def test_cancelled_connect_cleans_open_socket(self) -> None:
        socket = FakeWaveLinkSocket(ignored_methods={"getApplicationInfo"})
        client = WaveLinkClient(rpc_timeout=10)
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(return_value=socket)

        with patch("wavelink_core.websockets.connect", connector):
            task = asyncio.create_task(client.connect())
            while not socket.sent:
                await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(socket.closed)
        self.assertIs(client.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(client.ws)
        self.assertEqual(client._pending, {})

    async def test_invalid_collection_response_raises_protocol_error(self) -> None:
        client = WaveLinkClient()
        client.call = AsyncMock(return_value={"channels": None})  # type: ignore[method-assign]

        with self.assertRaises(WaveLinkProtocolError):
            await client.get_channels()

    async def test_collection_items_require_string_ids(self) -> None:
        client = WaveLinkClient()
        client.call = AsyncMock(  # type: ignore[method-assign]
            return_value={"channels": [{"name": "Missing ID"}]}
        )

        with self.assertRaisesRegex(WaveLinkProtocolError, r"channels\[0\]\.id"):
            await client.get_channels()

    async def test_application_info_rejects_boolean_interface_revision(self) -> None:
        client = WaveLinkClient()
        client._call = AsyncMock(  # type: ignore[method-assign]
            return_value={"appID": "EWL", "interfaceRevision": True}
        )

        with self.assertRaisesRegex(WaveLinkProtocolError, "interfaceRevision"):
            await client.get_application_info()

    async def test_output_devices_require_the_documented_envelope(self) -> None:
        client = WaveLinkClient()
        client.call = AsyncMock(  # type: ignore[method-assign]
            return_value={"outputDevices": [{"id": "speakers"}]}
        )

        with self.assertRaisesRegex(WaveLinkProtocolError, "mainOutput"):
            await client.get_output_devices()

    async def test_valid_typed_response_remains_a_plain_dictionary(self) -> None:
        client = WaveLinkClient()
        response = {
            "mainOutput": {"outputDeviceId": "device", "outputId": "output"},
            "outputDevices": [{"id": "device", "vendorField": 42}],
        }
        client.call = AsyncMock(return_value=response)  # type: ignore[method-assign]

        self.assertIs(await client.get_output_devices(), response)

    async def test_mutation_response_must_be_an_object(self) -> None:
        client = WaveLinkClient()
        client.call = AsyncMock(return_value=None)  # type: ignore[method-assign]

        with self.assertRaisesRegex(WaveLinkProtocolError, "setMix"):
            await client.set_mix_mute("monitor", False)

    def test_clamp_rejects_non_finite_and_boolean_values(self) -> None:
        with self.assertRaises(ValueError):
            clamp01(float("nan"))
        with self.assertRaises(ValueError):
            clamp01(float("inf"))
        with self.assertRaises(TypeError):
            clamp01(True)


class WaveLinkReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_rpc_waits_for_reconnect(self) -> None:
        first_socket = FakeWaveLinkSocket()
        second_socket = FakeWaveLinkSocket(ignored_methods={"getApplicationInfo"})
        client = WaveLinkClient(reconnect_initial_delay=0.01)
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(side_effect=[first_socket, second_socket])

        with patch("wavelink_core.websockets.connect", connector):
            await client.connect()
            await first_socket.remote_close()
            async with asyncio.timeout(0.5):
                while not second_socket.sent:
                    await asyncio.sleep(0)

            waiting_call = asyncio.create_task(client.call("afterReconnect"))
            await asyncio.sleep(0)
            self.assertFalse(waiting_call.done())

            handshake = second_socket.sent[0]
            await second_socket.respond(
                handshake["id"], {"appID": "EWL", "interfaceRevision": 1}
            )

            self.assertEqual(
                await asyncio.wait_for(waiting_call, timeout=0.5),
                {"method": "afterReconnect"},
            )
            await client.close()

    async def test_remote_close_reconnects_and_restores_session(self) -> None:
        first_socket = FakeWaveLinkSocket()
        second_socket = FakeWaveLinkSocket()
        client = WaveLinkClient(
            reconnect_initial_delay=0.01,
            reconnect_max_delay=0.02,
        )
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(side_effect=[first_socket, second_socket])

        with patch("wavelink_core.websockets.connect", connector):
            await client.connect()
            await client.set_plugin_info(["SDPlus"])
            await client.subscribe_focused_app()
            await first_socket.remote_close()

            async with asyncio.timeout(0.5):
                while connector.await_count < 2:
                    await asyncio.sleep(0)
                await client.wait_until_connected()

            methods = [payload["method"] for payload in second_socket.sent]
            self.assertEqual(
                methods,
                ["getApplicationInfo", "setPluginInfo", "setSubscription"],
            )
            self.assertIs(client.state, ConnectionState.CONNECTED)
            self.assertEqual(client.connected_port, 1884)
            await client.close()

    async def test_reconnect_retries_after_failed_attempt(self) -> None:
        first_socket = FakeWaveLinkSocket()
        recovered_socket = FakeWaveLinkSocket()
        client = WaveLinkClient(
            reconnect_initial_delay=0.01,
            reconnect_max_delay=0.02,
        )
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(
            side_effect=[first_socket, OSError("app is restarting"), recovered_socket]
        )

        with patch("wavelink_core.websockets.connect", connector):
            await client.connect()
            with self.assertLogs("wavelink_core", level="WARNING"):
                await first_socket.remote_close()
                async with asyncio.timeout(0.5):
                    while connector.await_count < 3:
                        await asyncio.sleep(0.005)
                    await client.wait_until_connected()

            self.assertIs(client.state, ConnectionState.CONNECTED)
            self.assertEqual(connector.await_count, 3)
            await client.close()

    async def test_explicit_close_does_not_reconnect(self) -> None:
        socket = FakeWaveLinkSocket()
        client = WaveLinkClient(reconnect_initial_delay=0.01)
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(return_value=socket)

        with patch("wavelink_core.websockets.connect", connector):
            await client.connect()
            await client.close()
            await asyncio.sleep(0.03)

        connector.assert_awaited_once()
        self.assertIs(client.state, ConnectionState.DISCONNECTED)
        self.assertIsNone(client._reconnect_task)

    async def test_in_flight_rpc_is_not_replayed_after_reconnect(self) -> None:
        first_socket = FakeWaveLinkSocket(ignored_methods={"setChannel"})
        second_socket = FakeWaveLinkSocket()
        client = WaveLinkClient(reconnect_initial_delay=0.01)
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(side_effect=[first_socket, second_socket])

        with patch("wavelink_core.websockets.connect", connector):
            await client.connect()
            pending = asyncio.create_task(client.set_channel_mute("channel", True))
            while len(first_socket.sent) < 2:
                await asyncio.sleep(0)
            await first_socket.remote_close()

            with self.assertRaises(WaveLinkDisconnectedError):
                await pending
            async with asyncio.timeout(0.5):
                while connector.await_count < 2:
                    await asyncio.sleep(0)
                await client.wait_until_connected()

            self.assertNotIn(
                "setChannel", [payload["method"] for payload in second_socket.sent]
            )
            await client.close()


if __name__ == "__main__":
    unittest.main()
