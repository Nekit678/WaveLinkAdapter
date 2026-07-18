"""Public interface for the WaveLinkAdapter library.

The package exposes the asynchronous :class:`WaveLinkClient`, typed response
and update models, event callback types, connection state, and public
exceptions from one stable import location.

Typical usage::

    import asyncio
    from wavelink_adapter import WaveLinkClient

    async def main() -> None:
        async with WaveLinkClient() as client:
            for channel in await client.get_channels():
                print(channel.id, channel.name)

    asyncio.run(main())

Applications should prefer imports from ``wavelink_adapter``. The
``wavelink_adapter.client`` and ``wavelink_adapter.models`` modules remain
available when more focused API documentation or type discovery is useful.
"""

from .client import (
    ConnectionState,
    EventHandler,
    TypedEventHandler,
    WaveLinkClient,
    WaveLinkDisconnectedError,
    WaveLinkEvent,
    WaveLinkProtocolError,
    WaveLinkRpcError,
    WaveLinkTimeoutError,
    clamp01,
)
from .models import (
    Application,
    ApplicationInfo,
    Channel,
    ChannelMix,
    ChannelMixUpdate,
    ChannelUpdate,
    CreateProfileRequested,
    Effect,
    EffectUpdate,
    FocusedAppChanged,
    FocusedAppChannel,
    FocusedAppSubscription,
    IdentifiedObject,
    ImageInfo,
    Input,
    InputDevice,
    InputDeviceUpdate,
    InputUpdate,
    JsonModel,
    JsonScalar,
    JsonValue,
    LevelMeterChanged,
    LevelMeterSubscription,
    LevelMeterType,
    LevelValue,
    MainOutput,
    MeterEntry,
    Mix,
    MixUpdate,
    Output,
    OutputDevice,
    OutputDevices,
    OutputDeviceUpdate,
    OutputDeviceUpdateParams,
    OutputDeviceUpdateResult,
    OutputUpdate,
    PluginInfoResult,
    SetOutputDeviceParams,
    SubscriptionUpdate,
    WaveLinkSchemaError,
)

__all__ = [
    "Application",
    "ApplicationInfo",
    "Channel",
    "ChannelMix",
    "ChannelMixUpdate",
    "ChannelUpdate",
    "ConnectionState",
    "CreateProfileRequested",
    "Effect",
    "EffectUpdate",
    "EventHandler",
    "FocusedAppChanged",
    "FocusedAppChannel",
    "FocusedAppSubscription",
    "IdentifiedObject",
    "ImageInfo",
    "Input",
    "InputDevice",
    "InputDeviceUpdate",
    "InputUpdate",
    "JsonModel",
    "JsonScalar",
    "JsonValue",
    "LevelMeterChanged",
    "LevelMeterSubscription",
    "LevelMeterType",
    "LevelValue",
    "MainOutput",
    "MeterEntry",
    "Mix",
    "MixUpdate",
    "Output",
    "OutputDevice",
    "OutputDevices",
    "OutputDeviceUpdate",
    "OutputDeviceUpdateParams",
    "OutputDeviceUpdateResult",
    "OutputUpdate",
    "PluginInfoResult",
    "SetOutputDeviceParams",
    "SubscriptionUpdate",
    "TypedEventHandler",
    "WaveLinkClient",
    "WaveLinkDisconnectedError",
    "WaveLinkEvent",
    "WaveLinkProtocolError",
    "WaveLinkRpcError",
    "WaveLinkSchemaError",
    "WaveLinkTimeoutError",
    "clamp01",
]
