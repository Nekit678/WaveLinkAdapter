from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from wavelink_adapter import (
    ConnectionState,
    WaveLinkClient,
    WaveLinkDisconnectedError,
    WaveLinkProtocolError,
    WaveLinkRpcError,
    WaveLinkTimeoutError,
    clamp01,
)
from wavelink_adapter import (
    Channel,
    ChannelMix,
    ChannelUpdate,
    Effect,
    FocusedAppChanged,
    Input,
    InputDevice,
    InputUpdate,
    LevelMeterChanged,
    LevelValue,
    MainOutput,
    OutputDevices,
    SetOutputDeviceParams,
    WaveLinkSchemaError,
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
        elif method == "getInputDevices":
            result = {
                "inputDevices": [{"id": "input-device", "inputs": [{"id": "input"}]}]
            }
        elif method == "getOutputDevices":
            result = {
                "mainOutput": {
                    "outputDeviceId": "output-device",
                    "outputId": "output",
                },
                "outputDevices": [
                    {"id": "output-device", "outputs": [{"id": "output"}]}
                ],
            }
        elif method == "getChannels":
            result = {"channels": [{"id": "channel"}]}
        elif method == "getMixes":
            result = {"mixes": [{"id": "mix"}]}
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


class WaveLinkObjectSchemaTests(unittest.TestCase):
    def test_nested_response_supports_attributes_and_json_round_trip(self) -> None:
        raw = {
            "id": "microphone",
            "name": "Mic",
            "level": 0.75,
            "isMuted": False,
            "mixes": [{"id": "monitor", "level": 0.5}],
            "effects": [{"id": "noise-removal", "isEnabled": True}],
            "vendorField": {"revision": 3},
        }

        channel = Channel.from_dict(raw)

        self.assertEqual(channel.id, "microphone")
        self.assertEqual(channel.name, "Mic")
        self.assertIsInstance(channel.mixes[0], ChannelMix)
        self.assertEqual(channel.mixes[0].level, 0.5)
        self.assertIsInstance(channel.effects[0], Effect)
        self.assertTrue(channel.effects[0].is_enabled)
        self.assertEqual(channel.extra, {"vendorField": {"revision": 3}})
        self.assertEqual(channel.to_dict(), raw)

    def test_update_schema_uses_attributes_and_json_names(self) -> None:
        update = ChannelUpdate(id="channel", is_muted=True, level=0.4)

        self.assertEqual(
            update.to_dict(),
            {"id": "channel", "isMuted": True, "level": 0.4},
        )

    def test_channel_mix_accepts_id_and_mix_id_wire_shapes(self) -> None:
        current = ChannelMix.from_dict({"id": "monitor", "level": 0.5})
        documented = ChannelMix.from_dict({"mixId": "stream", "level": 0.7})

        self.assertEqual(current.identifier, "monitor")
        self.assertEqual(documented.identifier, "stream")
        self.assertEqual(current.to_dict(), {"id": "monitor", "level": 0.5})
        self.assertEqual(documented.to_dict(), {"mixId": "stream", "level": 0.7})

    def test_hardware_metadata_is_typed_and_round_trips(self) -> None:
        raw = {
            "id": "wave-device",
            "isWaveDevice": True,
            "inputs": [
                {
                    "id": "microphone",
                    "isGainLockOn": True,
                    "micPcMix": {"value": 0.25, "isInverted": True},
                    "effects": [
                        {
                            "id": "clipguard",
                            "isEnabled": True,
                            "isSupported": True,
                        }
                    ],
                }
            ],
        }

        device = InputDevice.from_dict(raw)

        self.assertTrue(device.is_wave_device)
        self.assertTrue(device.inputs[0].is_gain_lock_on)
        self.assertTrue(device.inputs[0].mic_pc_mix.is_inverted)
        self.assertTrue(device.inputs[0].effects[0].is_supported)
        self.assertEqual(device.to_dict(), raw)

    def test_nested_schema_rejects_invalid_field_types(self) -> None:
        with self.assertRaisesRegex(WaveLinkSchemaError, r"mixes\[0\]\.level"):
            Channel.from_dict(
                {"id": "channel", "mixes": [{"id": "mix", "level": "loud"}]}
            )

        with self.assertRaisesRegex(WaveLinkSchemaError, r"id must be str"):
            Channel(id=None)  # type: ignore[arg-type]

        with self.assertRaisesRegex(WaveLinkSchemaError, r"mixes\[0\].*ChannelMix"):
            Channel(  # type: ignore[list-item]
                id="channel",
                mixes=[{"id": "mix"}],
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
        await self.client.set_input_device(
            "device", [InputUpdate(id="input", is_muted=True)]
        )
        await self.client.set_output_device(
            SetOutputDeviceParams(
                main_output=MainOutput(
                    output_device_id="device",
                    output_id="output",
                )
            )
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

    async def test_setters_reject_legacy_dictionary_arguments(self) -> None:
        with self.assertRaisesRegex(TypeError, "InputUpdate"):
            await self.client.set_input_device(
                "device",
                [{"id": "input"}],  # type: ignore[list-item]
            )

        with self.assertRaisesRegex(TypeError, "ChannelUpdate"):
            await self.client.set_channel(  # type: ignore[arg-type]
                {"id": "channel", "isMuted": True}
            )

    async def test_input_convenience_methods_clamp_and_select_effect_collection(
        self,
    ) -> None:
        await self.client.set_input_gain("device", "input", 1.5)
        await self.client.set_input_mic_pc_mix("device", "input", -1)
        await self.client.set_input_gain_lock("device", "input", True)
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
                        "inputs": [{"id": "input", "isGainLockOn": True}],
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

    async def test_disabling_meter_removes_only_matching_saved_subscription(
        self,
    ) -> None:
        await self.client.subscribe_level_meter("channel", "all")
        await self.client.subscribe_level_meter("mix", "all")
        await self.client.subscribe_level_meter("channel", "all", False)

        self.assertEqual(
            list(self.client._desired_level_meter_subscriptions),
            [("mix", "all", None)],
        )

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
        patcher = patch("wavelink_adapter.client.websockets.connect", connector)
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
        with self.assertLogs("wavelink_adapter.client", level="ERROR"):
            await socket.notify("changed", {})
            await asyncio.wait_for(handled.wait(), timeout=0.2)

        self.assertEqual(await client.call("stillAlive"), {"method": "stillAlive"})

    async def test_typed_event_merges_cache_and_preserves_raw_handlers(self) -> None:
        client, socket, _ = await self.connect_fake()
        client._input_devices = [
            InputDevice(
                id="device",
                name="Wave XLR",
                inputs=[
                    Input(
                        id="microphone",
                        name="Mic",
                        gain=LevelValue(0.4),
                        is_muted=False,
                    )
                ],
            )
        ]
        typed_handled = asyncio.Event()
        raw_handled = asyncio.Event()
        received: list[InputDevice] = []

        def typed_handler(event: object) -> None:
            assert isinstance(event, InputDevice)
            received.append(event)
            typed_handled.set()

        client.on_typed("inputDeviceChanged", typed_handler)  # type: ignore[arg-type]
        client.on("inputDeviceChanged", lambda _: raw_handled.set())
        await socket.notify(
            "inputDeviceChanged",
            {
                "id": "device",
                "inputs": [{"id": "microphone", "isMuted": True}],
            },
        )

        await asyncio.wait_for(typed_handled.wait(), timeout=0.2)
        await asyncio.wait_for(raw_handled.wait(), timeout=0.2)
        cached = client.input_devices[0]
        self.assertEqual(cached.name, "Wave XLR")
        self.assertEqual(cached.inputs[0].name, "Mic")
        self.assertEqual(cached.inputs[0].gain.value, 0.4)
        self.assertTrue(cached.inputs[0].is_muted)
        self.assertIs(received[0], cached)

    async def test_level_meter_async_stream_is_typed_and_updates_cache(self) -> None:
        client, socket, _ = await self.connect_fake()
        stream = client.stream_level_meters(queue_size=2)
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)

        await socket.notify(
            "levelMeterChanged",
            {
                "channels": [
                    {
                        "id": "microphone",
                        "levelLeftPercentage": 25.0,
                        "levelRightPercentage": 30.0,
                    }
                ]
            },
        )

        event = await asyncio.wait_for(pending, timeout=0.2)
        self.assertIsInstance(event, LevelMeterChanged)
        self.assertEqual(event.channels[0].id, "microphone")
        self.assertEqual(event.channels[0].level_right_percentage, 30.0)
        self.assertIs(client.level_meters, event)
        await stream.aclose()
        self.assertNotIn("levelMeterChanged", client._typed_event_handlers)

    async def test_focused_app_typed_event_updates_cache(self) -> None:
        client, socket, _ = await self.connect_fake()
        handled = asyncio.Event()

        def handler(event: object) -> None:
            assert isinstance(event, FocusedAppChanged)
            handled.set()

        client.on_typed("focusedAppChanged", handler)  # type: ignore[arg-type]
        await socket.notify(
            "focusedAppChanged",
            {"id": "app", "name": "Player", "channel": {"id": "music"}},
        )

        await asyncio.wait_for(handled.wait(), timeout=0.2)
        self.assertEqual(client.focused_app.name, "Player")
        self.assertEqual(client.focused_app.channel.id, "music")

    async def test_getters_populate_state_cache(self) -> None:
        client = WaveLinkClient()
        client.call = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"channels": [{"id": "microphone"}]},
                {"mixes": [{"id": "monitor"}]},
                {"inputDevices": [{"id": "wave"}]},
            ]
        )

        await client.get_channels()
        await client.get_mixes()
        await client.get_input_devices()

        self.assertEqual(client.channels[0].id, "microphone")
        self.assertEqual(client.mixes[0].id, "monitor")
        self.assertEqual(client.input_devices[0].id, "wave")

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

        with patch("wavelink_adapter.client.websockets.connect", connector):
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

    async def test_valid_response_becomes_nested_object_schema(self) -> None:
        client = WaveLinkClient()
        response = {
            "mainOutput": {"outputDeviceId": "device", "outputId": "output"},
            "outputDevices": [{"id": "device", "vendorField": 42}],
        }
        client.call = AsyncMock(return_value=response)  # type: ignore[method-assign]

        state = await client.get_output_devices()

        self.assertIsInstance(state, OutputDevices)
        self.assertEqual(state.main_output.output_device_id, "device")
        self.assertEqual(state.output_devices[0].id, "device")
        self.assertEqual(state.output_devices[0].extra, {"vendorField": 42})
        self.assertEqual(state.to_dict(), response)

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

        with patch("wavelink_adapter.client.websockets.connect", connector):
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

        with patch("wavelink_adapter.client.websockets.connect", connector):
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

    async def test_reconnect_restores_every_level_meter_subscription(self) -> None:
        first_socket = FakeWaveLinkSocket()
        second_socket = FakeWaveLinkSocket()
        client = WaveLinkClient(reconnect_initial_delay=0.01)
        client.discover_ports = Mock(return_value=[1884])  # type: ignore[method-assign]
        connector = AsyncMock(side_effect=[first_socket, second_socket])

        with patch("wavelink_adapter.client.websockets.connect", connector):
            await client.connect()
            await client.try_subscribe_level_meters()
            await first_socket.remote_close()

            async with asyncio.timeout(0.5):
                while connector.await_count < 2:
                    await asyncio.sleep(0)
                await client.wait_until_connected()

            restored = [
                payload["params"]
                for payload in second_socket.sent
                if payload["method"] == "setSubscription"
            ]
            self.assertEqual(
                restored,
                [{"focusedAppChanged": {"isEnabled": True}}]
                + [
                    {
                        "levelMeterChanged": {
                            "type": meter_type,
                            "id": target_id,
                            "isEnabled": True,
                        }
                    }
                    for meter_type, target_id in (
                        ("input", "input"),
                        ("output", "output"),
                        ("channel", "channel"),
                        ("mix", "mix"),
                    )
                ],
            )
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

        with patch("wavelink_adapter.client.websockets.connect", connector):
            await client.connect()
            with self.assertLogs("wavelink_adapter.client", level="WARNING"):
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

        with patch("wavelink_adapter.client.websockets.connect", connector):
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

        with patch("wavelink_adapter.client.websockets.connect", connector):
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
