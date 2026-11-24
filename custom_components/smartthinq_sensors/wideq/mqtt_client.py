"""MQTT push client for LG SmartThinQ API v2 (AWS IoT Core)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import json
import logging
import os
import ssl
import tempfile
import time
from typing import Any
from urllib.parse import urlparse

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import paho.mqtt.client as mqtt

from . import core_exceptions as exc
from .core_async import SVC_CODE, ClientAsync
from .core_util import as_list

_LOGGER = logging.getLogger(__name__)

ROUTE_URL = "https://common.lgthinq.com/route"
CLIENT_REG_PATH = "service/users/client"
CLIENT_CERT_PATH = "service/users/client/certificate"

ANDROID_ROUTE_HEADERS = {
    "User-Agent": "okhttp/4.10.0",
    "x-Device-Platform": "AOS",
    "x-platform-type": "ANDROID/13",
    "x-iot-sdk-type": "aws-sdk-android",
}
_DASHBOARD_REFRESH = "dashboardRefresh"
_FUNC_SYNC_REFRESH = "funcSyncRefresh"

MQTT_KEEPALIVE = 60
CONNECT_TIMEOUT = 20
INITIAL_RETRY_DELAY = 10
MAX_RETRY_DELAY = 300
MAX_SESSION_AGE = 4 * 3600
SESSION_CHECK_INTERVAL = 60

# Amazon Trust Services Root CA 1 (used to verify AWS IoT Core broker)
AMAZON_ROOT_CA1_PEM = """-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
ADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6
b24gUm9vdCBDQSAxMB4XDTE1MDUyNjAwMDAwMFoXDTM4MDExNzAwMDAwMFowOTEL
MAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJv
b3QgQ0EgMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALJ4gHHKeNXj
ca9HgFB0fW7Y14h29Jlo91ghYPl0hAEvrAIthtOgQ3pOsqTQNroBvo3bSMgHFzZM
9O6II8c+6zf1tRn4SWiw3te5djgdYZ6k/oI2peVKVuRF4fn9tBb6dNqcmzU5L/qw
IFAGbHrQgLKm+a/sRxmPUDgH3KKHOVj4utWp+UhnMJbulHheb4mjUcAwhmahRWa6
VOujw5H5SNz/0egwLX0tdHA114gk957EWW67c4cX8jJGKLhD+rcdqsq08p8kDi1L
93FcXmn/6pUCyziKrlA4b9v7LWIbxcceVOF34GfID5yHI9Y/QCB/IIDEgEw+OyQm
jgSubJrIqg0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMC
AYYwHQYDVR0OBBYEFIQYzIU07LwMlJQuCFmcx7IQTgoIMA0GCSqGSIb3DQEBCwUA
A4IBAQCY8jdaQZChGsV2USggNiMOruYou6r4lK5IpDB/G/wkjUu0yKGX9rbxenDI
U5PMCCjjmCXPI6T53iHTfIUJrU6adTrCC2qJeHZERxhlbI1Bjjt/msv0tadQ1wUs
N+gDS63pYaACbvXy8MWy7Vu33PqUXHeeE6V/Uq2V8viTO96LXFvKWlJbYK8U90vv
o/ufQJVtMVT8QtPHRh8jrdkPSHCa2XV4cdFyQzR1bldZwgJcJmApzyMZFo6IQ6XU
5MsI+yMRQ+hDKXJioaldXgjUkK642M4UwtBV8ob2xJNDd2ZhwLnoQdeXeGADbkpy
rqXRfboQnoZsG4q5WTP468SQvvG5
-----END CERTIFICATE-----
"""


class ThinQMQTTClient:
    """Manage a persistent mTLS MQTT push connection to LG ThinQ AWS IoT."""

    def __init__(
        self,
        client: ClientAsync,
        on_push_callback: Callable[[str, dict[str, Any]], None],
        on_connect_callback: Callable[[], None] | None = None,
        on_refresh_callback: Callable[[str], None] | None = None,
    ) -> None:
        """Initialize the ThinQ MQTT push client."""
        self._client = client
        self._on_push_callback = on_push_callback
        self._on_connect_callback = on_connect_callback
        self._on_refresh_callback = on_refresh_callback

        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._disconnected_event = asyncio.Event()

        self._mqtt_client: mqtt.Client | None = None
        self._mqtt_server_url: str | None = None
        self._subscriptions: list[str] = []
        self._is_connected = False
        self._connect_rc: int | None = None
        self._connected_client_id: str | None = None
        self._last_message_ts: float = 0.0
        self._force_reconnect_requested = False

    @property
    def is_connected(self) -> bool:
        """Return True if the MQTT client is currently connected."""
        return self._is_connected

    @property
    def last_message_ts(self) -> float:
        """Return monotonic timestamp of the last received MQTT message."""
        return self._last_message_ts

    async def async_reconnect(self) -> None:
        """Force an immediate clean MQTT reconnection with fresh auth/cert."""
        self._force_reconnect_requested = True
        self._disconnected_event.set()

    async def async_start(self) -> None:
        """Start the MQTT connection supervision loop."""
        if self._runner_task and not self._runner_task.done():
            return
        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._runner_task = asyncio.create_task(self._run_loop())

    async def async_stop(self) -> None:
        """Stop the MQTT connection and clean up resources."""
        self._stop_event.set()
        self._disconnected_event.set()

        if self._runner_task:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            self._runner_task = None

        await self._cleanup_mqtt()

    async def _cleanup_mqtt(self) -> None:
        """Disconnect and stop the underlying Paho MQTT loop."""
        self._is_connected = False
        mqtt_client = self._mqtt_client
        self._mqtt_client = None
        if mqtt_client is None:
            return

        def _stop_sync() -> None:
            try:
                mqtt_client.disconnect()
            except Exception:  # pylint: disable=broad-except
                pass
            try:
                mqtt_client.loop_stop()
            except Exception:  # pylint: disable=broad-except
                pass

        await asyncio.to_thread(_stop_sync)

    async def _run_loop(self) -> None:
        """Maintain the MQTT connection with automatic backoff on failures."""
        retry_delay = INITIAL_RETRY_DELAY

        while not self._stop_event.is_set():
            self._force_reconnect_requested = False
            try:
                await self._connect_once()
                retry_delay = INITIAL_RETRY_DELAY
                session_start_ts = time.monotonic()

                # Supervise connection: wake on disconnect, client_id rotation, or 4h session renewal
                while not self._stop_event.is_set():
                    try:
                        await asyncio.wait_for(
                            self._disconnected_event.wait(),
                            timeout=SESSION_CHECK_INTERVAL,
                        )
                        break
                    except asyncio.TimeoutError:
                        if self._stop_event.is_set():
                            break
                        if (
                            self._connected_client_id
                            and self._client.client_id != self._connected_client_id
                        ):
                            _LOGGER.info(
                                "ThinQ client_id rotated, reconnecting MQTT with new session"
                            )
                            self._force_reconnect_requested = True
                            break
                        if (time.monotonic() - session_start_ts) >= MAX_SESSION_AGE:
                            _LOGGER.info(
                                "Renewing ThinQ MQTT session and certificate after %dh",
                                MAX_SESSION_AGE // 3600,
                            )
                            self._force_reconnect_requested = True
                            break
            except asyncio.CancelledError:
                raise
            except (
                exc.InvalidCredentialError,
                exc.NotLoggedInError,
                exc.TokenError,
            ) as err:
                _LOGGER.warning(
                    "ThinQ MQTT authentication failed, refreshing client: %s", err
                )
                try:
                    await self._client.refresh(refresh_gateway=True)
                except Exception as refresh_err:  # pylint: disable=broad-except
                    _LOGGER.debug("ThinQ client refresh failed: %s", refresh_err)
            except exc.ClientDisconnected:
                _LOGGER.debug("ThinQ client disconnected, stopping MQTT listener")
                break
            except Exception as err:  # pylint: disable=broad-except
                _LOGGER.warning("ThinQ MQTT connection error: %s", err)
            finally:
                await self._cleanup_mqtt()

            if self._stop_event.is_set():
                break

            if self._force_reconnect_requested:
                self._force_reconnect_requested = False
                continue

            _LOGGER.debug("Reconnecting ThinQ MQTT in %s seconds", retry_delay)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=retry_delay)
            except asyncio.TimeoutError:
                pass
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)

    @staticmethod
    async def _call_with_client_id_retry(
        coro_factory: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Retry an API call immediately if LG returns 9006/9012 and rotates client_id."""
        try:
            return await coro_factory()
        except exc.UseOfficialAPIError as err:
            _LOGGER.debug(
                "Received UseOfficialAPIError (%s), retrying immediately with refreshed client_id",
                err,
            )
            return await coro_factory()

    async def async_get_route(self) -> dict[str, Any]:
        """Discover the regional MQTT broker URL from LG ThinQ route API."""
        auth = self._client.auth
        result = await self._call_with_client_id_retry(
            lambda: auth.gateway.core.thinq2_get(
                ROUTE_URL,
                access_token=auth.access_token,
                user_number=auth.user_number,
                headers=ANDROID_ROUTE_HEADERS,
            )
        )
        if not isinstance(result, dict):
            raise exc.InvalidResponseError(f"Invalid route response: {result}")
        return result

    @staticmethod
    def _generate_csr() -> tuple[str, str]:
        """Generate an RSA 2048 private key and X.509 CSR in PEM format."""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(
                x509.Name(
                    [
                        x509.NameAttribute(NameOID.COMMON_NAME, "AWS IoT Certificate"),
                        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Amazon"),
                    ]
                )
            )
            .sign(private_key, hashes.SHA256())
        )
        key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("utf-8")
        csr_pem = csr.public_bytes(serialization.Encoding.PEM).decode("utf-8")
        return key_pem, csr_pem

    @staticmethod
    def _normalize_cert_pem(cert_pem: str) -> str:
        """Ensure returned certificate string is a valid PEM block."""
        cert_pem = cert_pem.strip()
        if "BEGIN CERTIFICATE" in cert_pem:
            return cert_pem + "\n"
        return f"-----BEGIN CERTIFICATE-----\n{cert_pem}\n-----END CERTIFICATE-----\n"

    async def async_register_client_and_get_cert(
        self,
    ) -> tuple[str, str, list[str]]:
        """Register client session and obtain a signed mTLS certificate."""
        await self._client.refresh_auth()

        try:
            await self._call_with_client_id_retry(
                lambda: self._client.session.post2(CLIENT_REG_PATH, {})
            )
        except (
            exc.InvalidCredentialError,
            exc.NotLoggedInError,
            exc.TokenError,
            exc.UseOfficialAPIError,
        ):
            raise
        except exc.APIError as err:
            # Client ID may already be registered on LG backend
            _LOGGER.debug("Client registration returned non-fatal error: %s", err)

        key_pem, csr_pem = await asyncio.to_thread(self._generate_csr)
        csr_clean = (
            csr_pem.replace("-----BEGIN CERTIFICATE REQUEST-----", "")
            .replace("-----END CERTIFICATE REQUEST-----", "")
            .replace("\r", "")
            .replace("\n", "")
            .strip()
        )

        try:
            cert_result = await self._call_with_client_id_retry(
                lambda: self._client.session.post2(
                    CLIENT_CERT_PATH,
                    {"service-code": SVC_CODE, "csr": csr_clean},
                )
            )
        except (
            exc.InvalidCredentialError,
            exc.NotLoggedInError,
            exc.TokenError,
            exc.UseOfficialAPIError,
        ):
            raise
        except (exc.InvalidRequestError, exc.APIError) as err:
            _LOGGER.debug(
                "Certificate request with stripped CSR failed (%s), retrying full PEM",
                err,
            )
            cert_result = await self._call_with_client_id_retry(
                lambda: self._client.session.post2(
                    CLIENT_CERT_PATH,
                    {"service-code": SVC_CODE, "csr": csr_pem},
                )
            )

        if not isinstance(cert_result, dict) or "certificatePem" not in cert_result:
            raise exc.InvalidResponseError(
                f"Invalid certificate response from LG ThinQ: {cert_result}"
            )

        cert_pem = self._normalize_cert_pem(cert_result["certificatePem"])
        subscriptions = [
            topic
            for topic in as_list(cert_result.get("subscriptions", []))
            if isinstance(topic, str) and topic
        ]
        if not subscriptions:
            raise exc.InvalidResponseError("No MQTT subscriptions returned by LG ThinQ")

        return cert_pem, key_pem, subscriptions

    @staticmethod
    def _create_ssl_context(
        ca_pem: str, cert_pem: str, key_pem: str, use_alpn: bool = False
    ) -> ssl.SSLContext:
        """Build a mutual-TLS SSLContext from PEM strings."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cadata=ca_pem)
        if use_alpn:
            context.set_alpn_protocols(["x-amzn-mqtt-ca"])

        cert_path: str | None = None
        key_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".pem", encoding="utf-8", delete=False
            ) as cert_file:
                cert_file.write(cert_pem)
                cert_path = cert_file.name

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".key", encoding="utf-8", delete=False
            ) as key_file:
                key_file.write(key_pem)
                key_path = key_file.name

            context.load_cert_chain(certfile=cert_path, keyfile=key_path)
        finally:
            for tmp_path in (cert_path, key_path):
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

        return context

    async def _connect_once(self) -> None:
        """Perform route discovery, cert provisioning, and MQTT connect."""
        self._connected_event.clear()
        self._disconnected_event.clear()
        self._connect_rc = None

        if not self._mqtt_server_url:
            route = await self.async_get_route()
            self._mqtt_server_url = route.get("mqttServer")
            if not self._mqtt_server_url:
                raise exc.InvalidResponseError(
                    f"Missing mqttServer in route response: {route}"
                )

        server_url = self._mqtt_server_url
        if "://" not in server_url:
            server_url = f"ssl://{server_url}"
        parsed = urlparse(server_url)
        host = parsed.hostname
        port = parsed.port or 8883
        if not host:
            raise exc.InvalidResponseError(
                f"Invalid mqttServer URL: {self._mqtt_server_url}"
            )

        cert_pem, key_pem, subscriptions = await self.async_register_client_and_get_cert()
        self._subscriptions = subscriptions

        client_id = self._client.client_id
        if not client_id:
            raise exc.AuthenticationError("ThinQ client_id is not initialized")

        ssl_context = await asyncio.to_thread(
            self._create_ssl_context,
            AMAZON_ROOT_CA1_PEM,
            cert_pem,
            key_pem,
            port == 443,
        )

        if hasattr(mqtt, "CallbackAPIVersion"):
            mqtt_client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                client_id=client_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
            )
        else:
            mqtt_client = mqtt.Client(
                client_id=client_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
            )

        self._connected_client_id = client_id

        mqtt_client.tls_set_context(ssl_context)
        mqtt_client.on_connect = self._on_connect
        mqtt_client.on_disconnect = self._on_disconnect
        mqtt_client.on_message = self._on_message

        self._mqtt_client = mqtt_client

        _LOGGER.debug("Connecting to ThinQ MQTT broker %s:%s", host, port)
        mqtt_client.loop_start()
        try:
            await asyncio.to_thread(mqtt_client.connect, host, port, MQTT_KEEPALIVE)
            await asyncio.wait_for(
                self._connected_event.wait(), timeout=CONNECT_TIMEOUT
            )
            if self._connect_rc not in (None, 0):
                raise exc.AuthenticationError(
                    f"ThinQ MQTT connection rejected with code {self._connect_rc}"
                )
        except Exception:
            await self._cleanup_mqtt()
            raise

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,  # pylint: disable=unused-argument
        flags: dict,  # pylint: disable=unused-argument
        rc: int,
    ) -> None:
        """Handle MQTT CONNACK and subscribe to ThinQ push topics."""
        self._connect_rc = rc
        if rc != 0:
            _LOGGER.warning("ThinQ MQTT connection rejected with code %s", rc)
            if self._loop:
                self._loop.call_soon_threadsafe(self._connected_event.set)
                self._loop.call_soon_threadsafe(self._disconnected_event.set)
            return

        self._is_connected = True
        for topic in self._subscriptions:
            _LOGGER.debug("Subscribing to ThinQ MQTT topic: %s", topic)
            client.subscribe(topic, qos=1)

        _LOGGER.info(
            "Connected to ThinQ MQTT push broker (%d topic(s) subscribed)",
            len(self._subscriptions),
        )
        if self._loop:
            self._loop.call_soon_threadsafe(self._connected_event.set)
            if self._on_connect_callback:
                self._loop.call_soon_threadsafe(self._on_connect_callback)

    def _on_disconnect(
        self,
        client: mqtt.Client,  # pylint: disable=unused-argument
        userdata: Any,  # pylint: disable=unused-argument
        rc: int,
    ) -> None:
        """Handle MQTT disconnection."""
        self._is_connected = False
        if rc != 0 and not self._stop_event.is_set():
            _LOGGER.warning("ThinQ MQTT unexpectedly disconnected (rc=%s)", rc)
        else:
            _LOGGER.debug("ThinQ MQTT disconnected (rc=%s)", rc)

        if self._loop:
            self._loop.call_soon_threadsafe(self._disconnected_event.set)

    def _on_message(
        self,
        client: mqtt.Client,  # pylint: disable=unused-argument
        userdata: Any,  # pylint: disable=unused-argument
        msg: mqtt.MQTTMessage,
    ) -> None:
        """Parse incoming MQTT push message and dispatch report to event loop."""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            _LOGGER.debug("Failed to decode ThinQ MQTT payload: %s", err)
            return

        if not isinstance(payload, dict):
            return

        self._last_message_ts = time.monotonic()

        device_id = payload.get("deviceId") or self._extract_device_id_from_topic(
            msg.topic
        )
        if not device_id:
            return

        data_block = payload.get("data")
        if isinstance(data_block, dict):
            state_block = data_block.get("state")
            if isinstance(state_block, dict):
                desired = state_block.get("desired")
                if isinstance(desired, dict) and (
                    desired.get(_DASHBOARD_REFRESH) == "true"
                    or desired.get(_FUNC_SYNC_REFRESH) == "true"
                ):
                    flag_name = (
                        _DASHBOARD_REFRESH
                        if desired.get(_DASHBOARD_REFRESH) == "true"
                        else _FUNC_SYNC_REFRESH
                    )
                    _LOGGER.debug(
                        "ThinQ MQTT %s flag received for device %s",
                        flag_name,
                        device_id,
                    )
                    if self._on_refresh_callback and self._loop:
                        self._loop.call_soon_threadsafe(
                            self._on_refresh_callback, device_id
                        )
                    return

        report = self._extract_report(payload)
        if not report:
            _LOGGER.debug(
                "Ignoring ThinQ MQTT message without state report for %s: %s",
                device_id,
                payload,
            )
            return

        _LOGGER.debug(
            "ThinQ MQTT push received on %s for device %s", msg.topic, device_id
        )
        if self._loop:
            self._loop.call_soon_threadsafe(self._on_push_callback, device_id, report)

    @staticmethod
    def _extract_device_id_from_topic(topic: str) -> str | None:
        """Fallback extraction of deviceId from MQTT topic segments."""
        parts = topic.split("/")
        for idx, part in enumerate(parts):
            if part in ("devices", "things") and idx + 1 < len(parts):
                return parts[idx + 1] or None
        return None

    @staticmethod
    def _extract_report(payload: dict[str, Any]) -> dict[str, Any] | None:
        """Extract device snapshot/report dictionary from MQTT payload."""
        data = payload.get("data")
        if isinstance(data, dict):
            state = data.get("state")
            if isinstance(state, dict) and isinstance(state.get("reported"), dict):
                return state["reported"]
            if payload.get("type") == "monitoring":
                return data

        if isinstance(payload.get("report"), dict):
            return payload["report"]

        if isinstance(payload.get("snapshot"), dict):
            return payload["snapshot"]

        return None
