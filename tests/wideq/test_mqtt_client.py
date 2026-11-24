"""Tests for ThinQMQTTClient and CoreAsync MQTT/OAuth lifecycle (Opportunities 1-3)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import importlib.util
import os
import ssl
import sys
from typing import Any
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp


def _load_wideq():
    """Import wideq directly even if homeassistant core is not installed locally."""
    if "custom_components.smartthinq_sensors.wideq" in sys.modules:
        return sys.modules["custom_components.smartthinq_sensors.wideq"]
    if importlib.util.find_spec("homeassistant") is not None:
        import custom_components.smartthinq_sensors.wideq as pkg

        return pkg

    wideq_dir = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "custom_components",
            "smartthinq_sensors",
            "wideq",
        )
    )
    pkg_name = "wideq_under_test"
    if pkg_name in sys.modules:
        return sys.modules[pkg_name]
    spec = importlib.util.spec_from_file_location(
        pkg_name,
        os.path.join(wideq_dir, "__init__.py"),
        submodule_search_locations=[wideq_dir],
    )
    assert spec and spec.loader
    pkg = importlib.util.module_from_spec(spec)
    sys.modules[pkg_name] = pkg
    spec.loader.exec_module(pkg)
    return pkg


_wideq = _load_wideq()
_PKG = _wideq.__name__
core_async_mod = importlib.import_module(f"{_PKG}.core_async")
core_exc = importlib.import_module(f"{_PKG}.core_exceptions")
mqtt_mod = importlib.import_module(f"{_PKG}.mqtt_client")

Auth = core_async_mod.Auth
ClientAsync = core_async_mod.ClientAsync
CoreAsync = core_async_mod.CoreAsync
Gateway = core_async_mod.Gateway
ThinQMQTTClient = mqtt_mod.ThinQMQTTClient
MQTT_DEVICE_TYPE = mqtt_mod.MQTT_DEVICE_TYPE
CLIENT_REG_PATH = mqtt_mod.CLIENT_REG_PATH
CLIENT_CERT_PATH = mqtt_mod.CLIENT_CERT_PATH


class FakePahoClient:
    """Controllable stand-in for paho.mqtt.client.Client."""

    instances: list["FakePahoClient"] = []
    suback_qos: tuple[int, ...] = (1,)
    auto_suback: bool = True
    connect_rc: int = 0
    subscribe_rc: int = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.client_id = kwargs.get("client_id")
        self.clean_session = kwargs.get("clean_session")
        self.on_connect = None
        self.on_subscribe = None
        self.on_disconnect = None
        self.on_message = None
        self.subscribed_topics: list[Any] = []
        self.disconnected = False
        self.loop_stopped = False
        self.socket_alive = True
        self._next_mid = 42
        FakePahoClient.instances.append(self)

    def tls_set_context(self, context: Any) -> None:
        self.ssl_context = context

    def loop_start(self) -> None:
        pass

    def loop_stop(self) -> None:
        self.loop_stopped = True

    def is_connected(self) -> bool:
        return self.socket_alive and not self.disconnected

    def connect(self, host: str, port: int, keepalive: int) -> int:
        self.host = host
        self.port = port
        self.keepalive = keepalive
        if self.on_connect:
            self.on_connect(self, None, {}, FakePahoClient.connect_rc)
        return 0

    def subscribe(self, topic: Any, qos: int = 0) -> tuple[int, int]:
        self.subscribed_topics.append(topic)
        mid = self._next_mid
        self._next_mid += 1
        if FakePahoClient.subscribe_rc == 0 and FakePahoClient.auto_suback:
            if self.on_subscribe:
                self.on_subscribe(self, None, mid, FakePahoClient.suback_qos)
        return FakePahoClient.subscribe_rc, mid

    def disconnect(self) -> int:
        self.disconnected = True
        self.socket_alive = False
        return 0


def _build_mock_thinq_client(
    rest_client_id: str = "persisted-rest-client-id",
    access_token: str = "token-initial",
    seconds_left: float = 3500.0,
) -> MagicMock:
    """Create a mocked ClientAsync with realistic session/auth interfaces."""
    client = MagicMock()
    client.client_id = rest_client_id

    auth = MagicMock()
    auth.access_token = access_token
    auth.user_number = "BR2202037225985"
    auth.seconds_until_expiration = MagicMock(return_value=seconds_left)
    auth.gateway.core.thinq2_get = AsyncMock(
        return_value={"mqttServer": "ssl://a3phael99lf879-ats.iot.us-west-2.amazonaws.com:8883"}
    )
    client.auth = auth

    async def _refresh_auth(force_refresh: bool = False) -> None:
        if force_refresh:
            auth.access_token = f"{auth.access_token}-forced"
            auth.seconds_until_expiration.return_value = 3600.0

    client.refresh_auth = AsyncMock(side_effect=_refresh_auth)
    client.refresh = AsyncMock()

    async def _post2(
        path: str, data: dict | None = None, *, headers: dict | None = None
    ) -> dict:
        if path == CLIENT_REG_PATH:
            return {}
        if path == CLIENT_CERT_PATH:
            cid = (headers or {}).get("x-client-id", "unknown")
            return {
                "certificatePem": "FAKECERTDATA",
                "subscriptions": [f"aic/users/BR2202037225985/clients/{cid}/inbox"],
            }
        return {}

    client.session.post2 = AsyncMock(side_effect=_post2)
    client.session.delete2 = AsyncMock(return_value={})
    return client


class TestMQTTClientOpportunities(unittest.IsolatedAsyncioTestCase):
    """Test suite covering Opportunities 1, 2, and 3."""

    def setUp(self) -> None:
        FakePahoClient.instances.clear()
        FakePahoClient.suback_qos = (1,)
        FakePahoClient.auto_suback = True
        FakePahoClient.connect_rc = 0
        FakePahoClient.subscribe_rc = 0

    # ------------------------------------------------------------------
    # Opportunity 1: Dedicated mqtt_client_id + x-device-type: 601 + DELETE
    # ------------------------------------------------------------------

    async def test_dedicated_mqtt_client_id_and_device_type_header(self) -> None:
        """MQTT session must use a fresh 64-hex ID distinct from REST client_id and send x-device-type: 601."""
        thinq_client = _build_mock_thinq_client(rest_client_id="rest-fixed-id-123")
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)
        mqtt_client._loop = asyncio.get_running_loop()

        with patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client._connect_once()

        self.assertTrue(mqtt_client.is_connected)
        dedicated_id = mqtt_client.mqtt_client_id
        self.assertIsNotNone(dedicated_id)
        assert dedicated_id is not None
        self.assertEqual(len(dedicated_id), 64)
        self.assertNotEqual(dedicated_id, "rest-fixed-id-123")
        self.assertEqual(thinq_client.client_id, "rest-fixed-id-123")

        post_calls = thinq_client.session.post2.call_args_list
        self.assertEqual(len(post_calls), 2)

        reg_call = post_calls[0]
        self.assertEqual(reg_call.args[0], CLIENT_REG_PATH)
        self.assertEqual(
            reg_call.kwargs["headers"],
            {"x-client-id": dedicated_id, "x-device-type": MQTT_DEVICE_TYPE},
        )

        cert_call = post_calls[1]
        self.assertEqual(cert_call.args[0], CLIENT_CERT_PATH)
        self.assertEqual(
            cert_call.kwargs["headers"],
            {"x-client-id": dedicated_id, "x-device-type": MQTT_DEVICE_TYPE},
        )

        self.assertEqual(len(FakePahoClient.instances), 1)
        self.assertEqual(FakePahoClient.instances[0].client_id, dedicated_id)

        await mqtt_client.async_stop()

    async def test_core_async_custom_x_client_id_does_not_rotate_rest_id_on_9006(
        self,
    ) -> None:
        """9006 on a custom x-client-id header must not mutate the persisted REST client_id."""
        core = CoreAsync(country="BR", language="pt-BR", client_id="persisted-rest-id")
        with self.assertRaises(core_exc.UseOfficialAPIError):
            core._manage_lge_result(
                {"resultCode": "9006", "result": "official api"},
                True,
                "BR123",
                rotate_client_id=False,
            )
        self.assertEqual(core.client_id, "persisted-rest-id")

    async def test_session_renewal_deregisters_previous_mqtt_client_id(self) -> None:
        """Reconnecting must DELETE the previous mqtt_client_id and register a brand new one."""
        thinq_client = _build_mock_thinq_client()
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)
        mqtt_client._loop = asyncio.get_running_loop()

        with patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client._connect_once()
            first_id = mqtt_client.mqtt_client_id
            self.assertIsNotNone(first_id)

            await mqtt_client._cleanup_mqtt()
            await mqtt_client._connect_once()
            second_id = mqtt_client.mqtt_client_id

        self.assertIsNotNone(second_id)
        self.assertNotEqual(first_id, second_id)
        thinq_client.session.delete2.assert_called_once_with(
            CLIENT_REG_PATH,
            headers={"x-client-id": first_id, "x-device-type": MQTT_DEVICE_TYPE},
        )
        await mqtt_client.async_stop()

    async def test_stop_deregister_ignores_network_errors(self) -> None:
        """Network failures during DELETE service/users/client on stop must never propagate."""
        thinq_client = _build_mock_thinq_client()
        thinq_client.session.delete2 = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("WAN offline")
        )
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)
        mqtt_client._loop = asyncio.get_running_loop()

        with patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client._connect_once()
            # Must not raise aiohttp.ClientConnectionError
            await mqtt_client.async_stop()

        self.assertFalse(mqtt_client.is_connected)
        self.assertEqual(thinq_client.session.delete2.call_count, 1)

    async def test_register_retries_with_fresh_id_on_9006(self) -> None:
        """If POST service/users/client returns 9006, it must retry with a newly generated mqtt_client_id."""
        thinq_client = _build_mock_thinq_client()
        seen_reg_ids: list[str] = []

        async def _post2_with_9006_once(
            path: str, data: dict | None = None, *, headers: dict | None = None
        ) -> dict:
            cid = (headers or {})["x-client-id"]
            if path == CLIENT_REG_PATH:
                seen_reg_ids.append(cid)
                if len(seen_reg_ids) == 1:
                    raise core_exc.UseOfficialAPIError("9006")
                return {}
            return {
                "certificatePem": "CERT",
                "subscriptions": [f"aic/clients/{cid}/inbox"],
            }

        thinq_client.session.post2 = AsyncMock(side_effect=_post2_with_9006_once)
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)
        mqtt_client._loop = asyncio.get_running_loop()

        with patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client._connect_once()

        self.assertEqual(len(seen_reg_ids), 2)
        self.assertNotEqual(seen_reg_ids[0], seen_reg_ids[1])
        self.assertEqual(mqtt_client.mqtt_client_id, seen_reg_ids[1])
        await mqtt_client.async_stop()

    # ------------------------------------------------------------------
    # Opportunity 2: OAuth access_token rotation sync & pre-expiry renewal
    # ------------------------------------------------------------------

    async def test_auth_seconds_until_expiration_and_force_refresh(self) -> None:
        """Auth.seconds_until_expiration and ClientAsync.refresh_auth(force_refresh=True) work accurately."""
        core = MagicMock()
        core.refresh_auth = AsyncMock(return_value=("fresh-access-token", "3600"))
        gw = Gateway(
            {
                "empUri": "https://emp.example.com",
                "empTermsUri": "https://terms.example.com",
                "empSpxUri": "https://spx.example.com",
                "thinq1Uri": "https://t1.example.com",
                "thinq2Uri": "https://t2.example.com",
            },
            core,
        )
        auth = Auth(
            gw,
            refresh_token="ref-tok",
            access_token="initial-tok",
            token_validity="3600",
            user_number="BR123",
        )
        auth._token_created_on = datetime.now(timezone.utc) - timedelta(seconds=3450)
        rem = auth.seconds_until_expiration()
        self.assertGreater(rem, 140.0)
        self.assertLessEqual(rem, 151.0)

        client = ClientAsync(auth=auth, session=auth.start_session())
        # Even though >60s remain (TOKEN_EXP_LIMIT=60), force_refresh=True must fetch a new token
        await client.refresh_auth(force_refresh=True)
        self.assertEqual(client.auth.access_token, "fresh-access-token")
        self.assertGreater(client.auth.seconds_until_expiration(), 3590.0)

    async def test_reconnects_when_access_token_rotates_externally(self) -> None:
        """When an HTTP poll rotates client.auth.access_token, MQTT supervision loop must renew session."""
        thinq_client = _build_mock_thinq_client(access_token="token-v1")
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)

        with patch.object(mqtt_mod, "SESSION_CHECK_INTERVAL", 0.03), patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client.async_start()
            await asyncio.sleep(0.05)
            self.assertEqual(len(FakePahoClient.instances), 1)
            first_mqtt_id = mqtt_client.mqtt_client_id

            # Simulate 15m safety HTTP poll rotating OAuth access_token
            thinq_client.auth.access_token = "token-v2"
            await asyncio.sleep(0.09)

            self.assertGreaterEqual(len(FakePahoClient.instances), 2)
            self.assertNotEqual(mqtt_client.mqtt_client_id, first_mqtt_id)
            self.assertEqual(mqtt_client._connected_access_token, "token-v2")
            await mqtt_client.async_stop()

    async def test_proactive_refresh_when_access_token_near_expiry(self) -> None:
        """When HTTP polling is skipped, MQTT loop must proactively force-refresh OAuth token before expiry."""
        thinq_client = _build_mock_thinq_client(
            access_token="token-expiring", seconds_left=3500.0
        )
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)

        with patch.object(mqtt_mod, "SESSION_CHECK_INTERVAL", 0.03), patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client.async_start()
            await asyncio.sleep(0.05)
            self.assertEqual(len(FakePahoClient.instances), 1)

            # Simulate OAuth token approaching 1h expiration (120s left <= 180s margin)
            thinq_client.auth.seconds_until_expiration.return_value = 120.0
            await asyncio.sleep(0.09)

            thinq_client.refresh_auth.assert_any_call(force_refresh=True)
            self.assertGreaterEqual(len(FakePahoClient.instances), 2)
            self.assertEqual(
                mqtt_client._connected_access_token, "token-expiring-forced"
            )
            await mqtt_client.async_stop()

    # ------------------------------------------------------------------
    # Opportunity 3: Real SUBACK validation & Paho socket liveness
    # ------------------------------------------------------------------

    async def test_not_connected_until_suback_succeeds(self) -> None:
        """is_connected must remain False after CONNACK until SUBACK arrives with valid QoS."""
        FakePahoClient.auto_suback = False
        thinq_client = _build_mock_thinq_client()
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)
        mqtt_client._loop = asyncio.get_running_loop()

        with patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            connect_task = asyncio.create_task(mqtt_client._connect_once())
            # Allow _connect_once to reach _wait_connected_and_subscribed after CONNACK
            for _ in range(20):
                await asyncio.sleep(0.01)
                if FakePahoClient.instances:
                    break

            paho_inst = FakePahoClient.instances[0]
            # CONNACK already fired, but SUBACK has not arrived yet!
            self.assertFalse(mqtt_client.is_connected)
            self.assertFalse(connect_task.done())

            # Deliver SUBACK (mid=42, granted_qos=(1,))
            assert paho_inst.on_subscribe is not None
            paho_inst.on_subscribe(paho_inst, None, 42, (1,))
            await connect_task

            self.assertTrue(mqtt_client.is_connected)
            await mqtt_client.async_stop()

    async def test_suback_rejection_0x80_fails_connection(self) -> None:
        """Broker SUBACK 0x80 (rejected) must fail _connect_once, deregister client, and keep is_connected=False."""
        FakePahoClient.suback_qos = (0x80,)
        thinq_client = _build_mock_thinq_client()
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)
        mqtt_client._loop = asyncio.get_running_loop()

        with patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            with self.assertRaises(core_exc.AuthenticationError):
                await mqtt_client._connect_once()

        self.assertFalse(mqtt_client.is_connected)
        self.assertEqual(thinq_client.session.delete2.call_count, 1)

    async def test_paho_socket_drop_detected_by_is_connected_and_supervision_loop(
        self,
    ) -> None:
        """Silent Paho socket loss must immediately make is_connected=False and trigger reconnect."""
        thinq_client = _build_mock_thinq_client()
        mqtt_client = ThinQMQTTClient(thinq_client, lambda d, r: None)

        with patch.object(mqtt_mod, "SESSION_CHECK_INTERVAL", 0.03), patch.object(
            ThinQMQTTClient, "_generate_csr", return_value=("KEY_PEM", "CSR_PEM")
        ), patch.object(
            ThinQMQTTClient,
            "_create_ssl_context",
            return_value=MagicMock(spec=ssl.SSLContext),
        ), patch.object(mqtt_mod.mqtt, "Client", FakePahoClient):
            await mqtt_client.async_start()
            await asyncio.sleep(0.05)
            self.assertTrue(mqtt_client.is_connected)
            self.assertEqual(len(FakePahoClient.instances), 1)

            # Simulate silent socket death without Paho calling on_disconnect
            FakePahoClient.instances[0].socket_alive = False
            self.assertFalse(mqtt_client.is_connected)

            await asyncio.sleep(0.09)
            self.assertGreaterEqual(len(FakePahoClient.instances), 2)
            self.assertTrue(mqtt_client.is_connected)
            await mqtt_client.async_stop()
