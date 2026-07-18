from __future__ import annotations

import asyncio
import json
import unittest
import urllib.error
import urllib.request
from typing import Any

import websockets

from wavelink_adapter import ConnectionState, WaveLinkClient, WaveLinkRpcError
from examples.web_mixer.server import WaveLinkWebSocketServer


class FakeWaveLinkClient(WaveLinkClient):
    def __init__(self) -> None:
        super().__init__()
        self.state = ConnectionState.CONNECTED
        self.ws = object()  # type: ignore[assignment]
        self.connected_port = 1884
        self.calls: list[tuple[str, Any]] = []
        self.was_closed = False

    async def connect(self) -> None:
        self.state = ConnectionState.CONNECTED
        self.ws = object()  # type: ignore[assignment]

    async def close(self) -> None:
        self.was_closed = True
        self.state = ConnectionState.DISCONNECTED
        self.ws = None

    async def call(
        self, method: str, params: Any = None, *, timeout: float | None = None
    ) -> Any:
        self.calls.append((method, params))
        if method == "fail":
            raise WaveLinkRpcError(
                "rejected", code=-32602, data={"field": "level"}
            )
        return {"method": method, "params": params}


class WaveLinkWebSocketServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = FakeWaveLinkClient()
        self.server = WaveLinkWebSocketServer(self.client, port=0)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        await self.server.close()

    async def test_forwards_wave_link_rpc_and_preserves_id(self) -> None:
        response = await self.server.process_message(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": "web-1",
                    "method": "setChannel",
                    "params": {"id": "music", "level": 0.4},
                }
            )
        )

        self.assertEqual(
            response,
            {
                "jsonrpc": "2.0",
                "id": "web-1",
                "result": {
                    "method": "setChannel",
                    "params": {"id": "music", "level": 0.4},
                },
            },
        )
        self.assertEqual(
            self.client.calls,
            [("setChannel", {"id": "music", "level": 0.4})],
        )

    async def test_supports_status_batch_notifications_and_errors(self) -> None:
        response = await self.server.process_message(
            json.dumps(
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "server.getStatus",
                    },
                    {
                        "jsonrpc": "2.0",
                        "method": "notificationOnly",
                        "params": {},
                    },
                    {"jsonrpc": "2.0", "id": 2, "method": "fail"},
                ]
            )
        )

        assert isinstance(response, list)
        self.assertTrue(response[0]["result"]["connected"])
        self.assertEqual(response[0]["result"]["waveLinkPort"], 1884)
        self.assertEqual(
            response[1],
            {
                "jsonrpc": "2.0",
                "id": 2,
                "error": {
                    "code": -32602,
                    "message": "rejected",
                    "data": {"field": "level"},
                },
            },
        )
        self.assertIn(("notificationOnly", {}), self.client.calls)

    async def test_reports_parse_and_unknown_server_method_errors(self) -> None:
        parse_error = await self.server.process_message("{")
        missing_method = await self.server.process_message(
            '{"jsonrpc":"2.0","id":7,"method":"server.missing"}'
        )

        self.assertEqual(parse_error["error"]["code"], -32700)  # type: ignore[index]
        self.assertEqual(missing_method["error"]["code"], -32601)  # type: ignore[index]

    async def test_real_websocket_receives_welcome_response_and_event(self) -> None:
        port = self.server.bound_port
        self.assertIsNotNone(port)
        async with websockets.connect(
            f"ws://127.0.0.1:{port}", origin="http://localhost:3000"
        ) as socket:
            welcome = json.loads(await socket.recv())
            self.assertEqual(welcome["method"], "server.connectionChanged")
            self.assertTrue(welcome["params"]["connected"])

            await socket.send(
                '{"jsonrpc":"2.0","id":3,"method":"getChannels"}'
            )
            response = json.loads(await socket.recv())
            self.assertEqual(response["id"], 3)
            self.assertEqual(response["result"]["method"], "getChannels")

            await self.client._dispatch_event("channelChanged", {"id": "music"})
            event = json.loads(await asyncio.wait_for(socket.recv(), timeout=1))
            self.assertEqual(event["method"], "channelChanged")
            self.assertEqual(event["params"], {"id": "music"})

    async def test_accepts_private_network_browser_origin_for_tablets(self) -> None:
        port = self.server.bound_port
        self.assertIsNotNone(port)
        async with websockets.connect(
            f"ws://127.0.0.1:{port}", origin="http://192.168.1.25:8765"
        ) as socket:
            welcome = json.loads(await socket.recv())
            self.assertEqual(welcome["method"], "server.connectionChanged")

    async def test_serves_bundled_web_client_and_rejects_missing_assets(self) -> None:
        port = self.server.bound_port
        self.assertIsNotNone(port)

        def fetch(path: str) -> tuple[int, str, bytes]:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}{path}", timeout=1
                ) as response:
                    return (
                        response.status,
                        response.headers.get_content_type(),
                        response.read(),
                    )
            except urllib.error.HTTPError as exc:
                return exc.code, exc.headers.get_content_type(), exc.read()

        index, script, missing = await asyncio.gather(
            asyncio.to_thread(fetch, "/"),
            asyncio.to_thread(fetch, "/app.js"),
            asyncio.to_thread(fetch, "/missing.js"),
        )

        self.assertEqual(index[0:2], (200, "text/html"))
        self.assertIn(b"WAVELINK", index[2])
        self.assertEqual(script[0:2], (200, "text/javascript"))
        self.assertIn(b'sendRpc("setChannel"', script[2])
        for method in (
            b"setInputDevice",
            b"setOutputDevice",
            b"setMix",
            b"addToChannel",
            b"setPluginInfo",
            b"setSubscription",
        ):
            self.assertIn(method, script[2])
        self.assertEqual(missing[0], 404)


if __name__ == "__main__":
    unittest.main()
