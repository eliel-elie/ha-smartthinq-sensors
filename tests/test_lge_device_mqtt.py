"""Tests for LGEDevice & AirConditionerDevice power-on event enable and MQTT watchdog (Opportunity 4)."""

from __future__ import annotations

import asyncio
from enum import StrEnum
import importlib.util
import sys
import types
from typing import Any
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


def _install_ha_stubs_if_needed() -> None:
    """Install lightweight Home Assistant stubs when running outside full HA test env."""
    if importlib.util.find_spec("homeassistant") is not None:
        return

    ha = types.ModuleType("homeassistant")
    ha.__version__ = "2025.1.0"

    ha_components = types.ModuleType("homeassistant.components")
    ha_pn = types.ModuleType("homeassistant.components.persistent_notification")
    ha_pn.async_create = MagicMock()
    ha_pn.async_dismiss = MagicMock()
    ha_components.persistent_notification = ha_pn

    ha_ce = types.ModuleType("homeassistant.config_entries")
    ha_ce.SOURCE_IMPORT = "import"
    ha_ce.ConfigEntry = object

    ha_const = types.ModuleType("homeassistant.const")
    ha_const.CONF_CLIENT_ID = "client_id"
    ha_const.CONF_REGION = "region"
    ha_const.CONF_TOKEN = "token"
    ha_const.EVENT_HOMEASSISTANT_STOP = "homeassistant_stop"
    ha_const.MAJOR_VERSION = 2025
    ha_const.MINOR_VERSION = 1
    ha_const.__version__ = "2025.1.0"

    class Platform(StrEnum):
        BINARY_SENSOR = "binary_sensor"
        BUTTON = "button"
        CLIMATE = "climate"
        FAN = "fan"
        HUMIDIFIER = "humidifier"
        LIGHT = "light"
        NUMBER = "number"
        SELECT = "select"
        SENSOR = "sensor"
        SWITCH = "switch"
        WATER_HEATER = "water_heater"

    class UnitOfTemperature(StrEnum):
        CELSIUS = "°C"
        FAHRENHEIT = "°F"

    ha_const.Platform = Platform
    ha_const.UnitOfTemperature = UnitOfTemperature

    ha_core = types.ModuleType("homeassistant.core")
    ha_core.Event = object
    ha_core.HomeAssistant = object
    ha_core.callback = lambda fn: fn

    ha_exc = types.ModuleType("homeassistant.exceptions")
    ha_exc.ConfigEntryAuthFailed = Exception
    ha_exc.ConfigEntryNotReady = Exception

    ha_helpers = types.ModuleType("homeassistant.helpers")
    ha_dr = types.ModuleType("homeassistant.helpers.device_registry")
    ha_dr.CONNECTION_NETWORK_MAC = "mac"
    ha_dr.format_mac = lambda m: m
    ha_dr.async_get = MagicMock()
    ha_dr.async_entries_for_config_entry = MagicMock(return_value=[])
    ha_helpers.device_registry = ha_dr

    ha_aiohttp = types.ModuleType("homeassistant.helpers.aiohttp_client")
    ha_aiohttp.async_get_clientsession = MagicMock()

    ha_disp = types.ModuleType("homeassistant.helpers.dispatcher")
    ha_disp.async_dispatcher_connect = MagicMock()
    ha_disp.async_dispatcher_send = MagicMock()

    ha_entity = types.ModuleType("homeassistant.helpers.entity")
    ha_entity.DeviceInfo = dict

    ha_event = types.ModuleType("homeassistant.helpers.event")
    ha_event.async_track_time_interval = MagicMock()

    ha_uc = types.ModuleType("homeassistant.helpers.update_coordinator")

    class FakeDataUpdateCoordinator:
        def __init__(self, hass: Any, logger: Any, *, name: str, update_method: Any, update_interval: Any) -> None:
            self.hass = hass
            self.name = name
            self.update_method = update_method
            self.update_interval = update_interval
            self.data = None

        async def async_refresh(self) -> None:
            self.data = await self.update_method()

        async def async_request_refresh(self) -> None:
            self.data = await self.update_method()

        def async_set_updated_data(self, data: Any) -> None:
            self.data = data

    ha_uc.DataUpdateCoordinator = FakeDataUpdateCoordinator

    modules = {
        "homeassistant": ha,
        "homeassistant.components": ha_components,
        "homeassistant.components.persistent_notification": ha_pn,
        "homeassistant.config_entries": ha_ce,
        "homeassistant.const": ha_const,
        "homeassistant.core": ha_core,
        "homeassistant.exceptions": ha_exc,
        "homeassistant.helpers": ha_helpers,
        "homeassistant.helpers.device_registry": ha_dr,
        "homeassistant.helpers.aiohttp_client": ha_aiohttp,
        "homeassistant.helpers.dispatcher": ha_disp,
        "homeassistant.helpers.entity": ha_entity,
        "homeassistant.helpers.event": ha_event,
        "homeassistant.helpers.update_coordinator": ha_uc,
    }
    sys.modules.update(modules)


_install_ha_stubs_if_needed()

from custom_components.smartthinq_sensors import LGEDevice  # noqa: E402
from custom_components.smartthinq_sensors.const import DOMAIN, MQTT_CLIENT  # noqa: E402
from custom_components.smartthinq_sensors.wideq import DeviceType, PlatformType  # noqa: E402
from custom_components.smartthinq_sensors.wideq.devices.ac import (  # noqa: E402
    AirConditionerDevice,
)


class FakeStatus:
    """Minimal AC status object supporting in-place mutation like AirConditionerStatus."""

    def __init__(self, is_on: bool = False) -> None:
        self.is_on = is_on
        self.device_features: dict[str, Any] = {}


class FakeHass:
    """Minimal HomeAssistant loop wrapper for LGEDevice unit tests."""

    def __init__(self, mqtt_client: Any = None) -> None:
        self.data: dict[str, Any] = {DOMAIN: {}}
        if mqtt_client is not None:
            self.data[DOMAIN][MQTT_CLIENT] = mqtt_client
        self.created_tasks: list[asyncio.Task] = []

    def async_create_task(self, coro: Any) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self.created_tasks.append(task)
        return task


def _build_fake_ac_thinq_device(initial_on: bool = False) -> MagicMock:
    """Build a mocked ThinQ2 AirConditionerDevice."""
    dev = MagicMock()
    dev.name = "Ar Júlia"
    dev.unique_id = "a71fc6b6-fe0f-1057-a3b3-402f86ace20f"
    dev.device_info.device_id = dev.unique_id
    dev.device_info.type = DeviceType.AC
    dev.device_info.platform_type = PlatformType.THINQ2
    dev.device_info.macaddress = None
    dev.device_info.firmware = "1.0"
    dev.device_info.model_name = "RAC_056905_WW"
    dev.model_info.model_type = "RAC"
    dev.available_features = {}

    status = FakeStatus(is_on=initial_on)
    dev.status = status
    dev.init_device_info = AsyncMock(return_value=True)
    dev.poll = AsyncMock(return_value=status)
    dev.apply_push_snapshot = MagicMock(return_value=True)
    dev.async_enable_events = AsyncMock(return_value=True)
    dev.reset_event_enable_cooldown = MagicMock()
    return dev


class TestLGEDeviceMQTTOpportunity4(unittest.IsolatedAsyncioTestCase):
    """Test suite covering Opportunity 4 (power-on transitions & watchdog recovery)."""

    async def test_ac_pre_update_v2_does_not_consume_event_cooldown(self) -> None:
        """AirConditionerDevice._pre_update_v2 must be a no-op so LGEDevice owns watched event enables."""
        ac_dev = object.__new__(AirConditionerDevice)
        ac_dev._last_event_enable_ts = 123.45
        ac_dev.async_enable_events = AsyncMock(return_value=True)  # type: ignore[method-assign]

        await ac_dev._pre_update_v2()
        ac_dev.async_enable_events.assert_not_called()

        ac_dev.reset_event_enable_cooldown()
        self.assertEqual(ac_dev._last_event_enable_ts, 0.0)

    async def test_http_poll_off_to_on_triggers_enable_events_and_watchdog_immediately(
        self,
    ) -> None:
        """When safety HTTP poll discovers OFF -> ON, it must immediately schedule allEventEnable + watchdog."""
        mqtt_mock = MagicMock()
        mqtt_mock.is_connected = True
        mqtt_mock.last_message_ts = 10.0
        mqtt_mock.async_reconnect = AsyncMock()

        hass = FakeHass(mqtt_client=mqtt_mock)
        thinq_dev = _build_fake_ac_thinq_device(initial_on=False)
        lge_dev = LGEDevice(thinq_dev, hass)  # type: ignore[arg-type]
        await lge_dev.init_device()

        self.assertFalse(lge_dev._last_known_is_on)
        thinq_dev.async_enable_events.assert_not_called()

        # Simulate AC turned on externally and discovered during forced/safety HTTP poll
        on_status = FakeStatus(is_on=True)
        thinq_dev.poll = AsyncMock(return_value=on_status)
        lge_dev._force_http_poll = True

        orig_sleep = asyncio.sleep

        async def _fast_sleep(delay: float) -> None:
            await orig_sleep(0.01)

        with patch(
            "custom_components.smartthinq_sensors.asyncio.sleep",
            side_effect=_fast_sleep,
        ):
            # Deliver an MQTT push right after allEventEnable so watchdog sees healthy push
            async def _enable_and_bump_ts(**kwargs: Any) -> bool:
                mqtt_mock.last_message_ts = 25.0
                return True

            thinq_dev.async_enable_events = AsyncMock(side_effect=_enable_and_bump_ts)
            await lge_dev._async_state_update()
            await orig_sleep(0.06)

        thinq_dev.async_enable_events.assert_called_once_with(
            timeout=70, min_interval=0
        )
        mqtt_mock.async_reconnect.assert_not_called()
        self.assertTrue(lge_dev._last_known_is_on)
        await lge_dev.async_cancel_confirmation_poll()

    async def test_ha_local_turn_on_detects_transition_despite_inplace_mutation(
        self,
    ) -> None:
        """Turning on AC in HA mutates state.is_on in-place before async_set_updated; _last_known_is_on must catch it."""
        mqtt_mock = MagicMock()
        mqtt_mock.is_connected = True
        mqtt_mock.last_message_ts = 50.0
        mqtt_mock.async_reconnect = AsyncMock()

        hass = FakeHass(mqtt_client=mqtt_mock)
        thinq_dev = _build_fake_ac_thinq_device(initial_on=False)
        lge_dev = LGEDevice(thinq_dev, hass)  # type: ignore[arg-type]
        await lge_dev.init_device()
        self.assertFalse(lge_dev._last_known_is_on)

        orig_sleep = asyncio.sleep

        async def _fast_sleep(delay: float) -> None:
            await orig_sleep(0.01)

        async def _enable_and_bump_ts(**kwargs: Any) -> bool:
            mqtt_mock.last_message_ts += 10.0
            return True

        thinq_dev.async_enable_events = AsyncMock(side_effect=_enable_and_bump_ts)

        with patch(
            "custom_components.smartthinq_sensors.asyncio.sleep",
            side_effect=_fast_sleep,
        ), patch.object(
            lge_dev, "schedule_confirmation_poll", wraps=lge_dev.schedule_confirmation_poll
        ) as confirm_spy:
            # Simulate AirConditionerDevice.power(True) mutating status in-place BEFORE async_set_updated()
            assert lge_dev.state is not None
            lge_dev.state.is_on = True
            lge_dev.async_set_updated()

            self.assertTrue(lge_dev._last_known_is_on)
            confirm_spy.assert_called_once_with(delay=15)
            await orig_sleep(0.06)

        thinq_dev.async_enable_events.assert_any_call(timeout=70, min_interval=0)
        mqtt_mock.async_reconnect.assert_not_called()
        await lge_dev.async_cancel_confirmation_poll()

    async def test_mqtt_push_off_to_on_triggers_enable_events_and_watchdog(
        self,
    ) -> None:
        """MQTT push transitioning AC OFF -> ON must schedule allEventEnable paired with watchdog."""
        mqtt_mock = MagicMock()
        mqtt_mock.is_connected = True
        mqtt_mock.last_message_ts = 100.0
        mqtt_mock.async_reconnect = AsyncMock()

        hass = FakeHass(mqtt_client=mqtt_mock)
        thinq_dev = _build_fake_ac_thinq_device(initial_on=False)
        lge_dev = LGEDevice(thinq_dev, hass)  # type: ignore[arg-type]
        await lge_dev.init_device()

        thinq_dev.poll = AsyncMock(return_value=FakeStatus(is_on=True))

        async def _enable_and_bump_ts(**kwargs: Any) -> bool:
            mqtt_mock.last_message_ts = 110.0
            return True

        thinq_dev.async_enable_events = AsyncMock(side_effect=_enable_and_bump_ts)

        orig_sleep = asyncio.sleep

        async def _fast_sleep(delay: float) -> None:
            await orig_sleep(0.01)

        with patch(
            "custom_components.smartthinq_sensors.asyncio.sleep",
            side_effect=_fast_sleep,
        ):
            await lge_dev.async_handle_mqtt_push({"airState.operation": 1.0})
            await orig_sleep(0.06)

        thinq_dev.async_enable_events.assert_called_once_with(
            timeout=70, min_interval=30
        )
        mqtt_mock.async_reconnect.assert_not_called()
        await lge_dev.async_cancel_confirmation_poll()

    async def test_mqtt_watchdog_reconnects_resets_cooldown_and_schedules_confirmation_poll(
        self,
    ) -> None:
        """If allEventEnable succeeds but no MQTT push arrives within 12s, watchdog must reconnect, re-arm, and poll."""
        mqtt_mock = MagicMock()
        mqtt_mock.is_connected = True
        mqtt_mock.last_message_ts = 200.0
        mqtt_mock.async_reconnect = AsyncMock()

        hass = FakeHass(mqtt_client=mqtt_mock)
        thinq_dev = _build_fake_ac_thinq_device(initial_on=True)
        lge_dev = LGEDevice(thinq_dev, hass)  # type: ignore[arg-type]
        await lge_dev.init_device()
        await lge_dev.async_cancel_confirmation_poll()
        thinq_dev.async_enable_events.reset_mock()

        orig_sleep = asyncio.sleep

        async def _fast_sleep(delay: float) -> None:
            await orig_sleep(0.01)

        with patch(
            "custom_components.smartthinq_sensors.asyncio.sleep",
            side_effect=_fast_sleep,
        ), patch.object(lge_dev, "schedule_confirmation_poll") as confirm_mock:
            # Trigger watched allEventEnable while last_message_ts stays stuck at 200.0
            await lge_dev._async_trigger_ac_events_and_watchdog(
                min_interval=240, delay=0
            )
            await orig_sleep(0.08)

        # Watchdog must have forced MQTT reconnect, reset cooldown, re-sent allEventEnable once, and scheduled HTTP confirmation
        mqtt_mock.async_reconnect.assert_called_once()
        thinq_dev.reset_event_enable_cooldown.assert_called_once()
        self.assertEqual(thinq_dev.async_enable_events.call_count, 2)
        confirm_mock.assert_called_once_with(delay=6)
        await lge_dev.async_cancel_confirmation_poll()
