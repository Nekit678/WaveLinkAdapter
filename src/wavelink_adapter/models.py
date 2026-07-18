"""Object schemas for the Wave Link JSON-RPC API.

Public RPC wrappers return dataclass instances with Python-style attribute
names.  Every model can be created from a Wave Link JSON object with
``from_dict()`` and serialized back with ``to_dict()``.  Unknown API fields are
kept in ``extra`` so a newer Wave Link version doesn't lose information.
"""

from __future__ import annotations

import math
from dataclasses import MISSING, dataclass, field, fields
from functools import cache
from types import UnionType
from typing import (
    Any,
    ForwardRef,
    Literal,
    Mapping,
    Self,
    TypeAlias,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class WaveLinkSchemaError(ValueError):
    """A value cannot be represented by a Wave Link object schema."""


def _json_field(name: str, *, default: Any = MISSING) -> Any:
    options: dict[str, Any] = {"metadata": {"json_name": name}}
    if default is not MISSING:
        options["default"] = default
    return field(**options)


def _path(parent: str, child: str) -> str:
    return f"{parent}.{child}" if parent else child


@cache
def _schema_hints(schema: type[Any]) -> dict[str, Any]:
    return get_type_hints(schema)


def _type_name(expected: Any) -> str:
    origin = get_origin(expected)
    if origin in (Union, UnionType):
        return " | ".join(_type_name(item) for item in get_args(expected))
    if origin is Literal:
        return " | ".join(repr(item) for item in get_args(expected))
    if origin is list:
        return f"list[{_type_name(get_args(expected)[0])}]"
    if origin is dict:
        key_type, value_type = get_args(expected)
        return f"dict[{_type_name(key_type)}, {_type_name(value_type)}]"
    return getattr(expected, "__name__", str(expected))


def _coerce_value(
    expected: Any,
    value: Any,
    path: str,
    *,
    parse_models: bool = False,
) -> Any:
    if expected is Any or expected is object:
        return value

    if expected == "JsonValue" or (
        isinstance(expected, ForwardRef) and expected.__forward_arg__ == "JsonValue"
    ):
        return _coerce_json_value(value, path)

    origin = get_origin(expected)
    args = get_args(expected)

    if origin in (Union, UnionType):
        if type(None) in args:
            if value is None:
                return None
            non_none = tuple(option for option in args if option is not type(None))
            if len(non_none) == 1:
                return _coerce_value(
                    non_none[0], value, path, parse_models=parse_models
                )

        errors: list[WaveLinkSchemaError] = []
        for option in args:
            try:
                return _coerce_value(option, value, path, parse_models=parse_models)
            except WaveLinkSchemaError as exc:
                errors.append(exc)
        raise WaveLinkSchemaError(
            f"{path} must be {_type_name(expected)}, got {type(value).__name__}"
        ) from errors[-1] if errors else None

    if origin is Literal:
        if not any(value == item and type(value) is type(item) for item in args):
            raise WaveLinkSchemaError(
                f"{path} must be {_type_name(expected)}, got {value!r}"
            )
        return value

    if origin is list:
        if not isinstance(value, list):
            raise WaveLinkSchemaError(
                f"{path} must be {_type_name(expected)}, got {type(value).__name__}"
            )
        item_type = args[0]
        return [
            _coerce_value(
                item_type,
                item,
                f"{path}[{index}]",
                parse_models=parse_models,
            )
            for index, item in enumerate(value)
        ]

    if origin is dict:
        if not isinstance(value, Mapping):
            raise WaveLinkSchemaError(
                f"{path} must be {_type_name(expected)}, got {type(value).__name__}"
            )
        key_type, value_type = args
        result: dict[Any, Any] = {}
        for key, item in value.items():
            converted_key = _coerce_value(
                key_type,
                key,
                f"{path}.<key>",
                parse_models=parse_models,
            )
            result[converted_key] = _coerce_value(
                value_type,
                item,
                _path(path, str(key)),
                parse_models=parse_models,
            )
        return result

    if isinstance(expected, type) and issubclass(expected, JsonModel):
        if isinstance(value, expected):
            return value
        if parse_models and isinstance(value, Mapping):
            return expected.from_dict(value, path=path)
        raise WaveLinkSchemaError(
            f"{path} must be {_type_name(expected)}, got {type(value).__name__}"
        )

    if expected is bool:
        if type(value) is not bool:
            raise WaveLinkSchemaError(
                f"{path} must be bool, got {type(value).__name__}"
            )
        return value

    if expected is int:
        if type(value) is not int:
            raise WaveLinkSchemaError(f"{path} must be int, got {type(value).__name__}")
        return value

    if expected is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise WaveLinkSchemaError(
                f"{path} must be float, got {type(value).__name__}"
            )
        number = float(value)
        if not math.isfinite(number):
            raise WaveLinkSchemaError(f"{path} must be finite")
        return number

    if expected is str:
        if not isinstance(value, str):
            raise WaveLinkSchemaError(f"{path} must be str, got {type(value).__name__}")
        return value

    if expected is type(None):
        if value is not None:
            raise WaveLinkSchemaError(
                f"{path} must be None, got {type(value).__name__}"
            )
        return None

    if not isinstance(value, expected):
        raise WaveLinkSchemaError(
            f"{path} must be {_type_name(expected)}, got {type(value).__name__}"
        )
    return value


def _coerce_json_value(value: Any, path: str) -> JsonValue:
    if type(value) is float and not math.isfinite(value):
        raise WaveLinkSchemaError(f"{path} must be finite")
    if value is None or type(value) in (str, int, float, bool):
        return value
    if isinstance(value, list):
        return [
            _coerce_json_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise WaveLinkSchemaError(
                    f"{path}.<key> must be str, got {type(key).__name__}"
                )
            result[key] = _coerce_json_value(item, _path(path, key))
        return result
    raise WaveLinkSchemaError(
        f"{path} must be a JSON value, got {type(value).__name__}"
    )


def _encode_value(expected: Any, value: Any, path: str) -> JsonValue:
    converted = _coerce_value(expected, value, path)
    if isinstance(converted, JsonModel):
        return converted.to_dict()
    if isinstance(converted, list):
        item_type = get_args(expected)[0] if get_origin(expected) is list else Any
        return [
            _encode_value(item_type, item, f"{path}[{index}]")
            for index, item in enumerate(converted)
        ]
    if isinstance(converted, dict):
        value_type = get_args(expected)[1] if get_origin(expected) is dict else Any
        return {
            str(key): _encode_value(value_type, item, _path(path, str(key)))
            for key, item in converted.items()
        }
    return converted


@dataclass(slots=True, kw_only=True)
class JsonModel:
    """Base class for typed Wave Link JSON objects."""

    extra: dict[str, JsonValue] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        hints = _schema_hints(type(self))
        for model_field in fields(self):
            if model_field.name == "extra":
                continue
            value = getattr(self, model_field.name)
            json_name = model_field.metadata.get("json_name", model_field.name)
            converted = _coerce_value(hints[model_field.name], value, json_name)
            setattr(self, model_field.name, converted)

        self.extra = _coerce_value(dict[str, JsonValue], self.extra, "extra")
        self._validate_model()

    def _validate_model(self) -> None:
        """Hook for constraints involving more than one field."""

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], *, path: str = "") -> Self:
        if not isinstance(value, Mapping):
            raise WaveLinkSchemaError(
                f"{path or cls.__name__} must be an object, got {type(value).__name__}"
            )

        hints = _schema_hints(cls)
        kwargs: dict[str, Any] = {}
        known_keys: set[str] = set()

        for model_field in fields(cls):
            if model_field.name == "extra":
                continue
            json_name = model_field.metadata.get("json_name", model_field.name)
            known_keys.add(json_name)
            field_path = _path(path, json_name)
            if json_name in value:
                kwargs[model_field.name] = _coerce_value(
                    hints[model_field.name],
                    value[json_name],
                    field_path,
                    parse_models=True,
                )
            elif (
                model_field.default is MISSING
                and model_field.default_factory is MISSING
            ):
                raise WaveLinkSchemaError(f"missing required field {field_path}")

        extras = {key: item for key, item in value.items() if key not in known_keys}
        kwargs["extra"] = _coerce_value(
            dict[str, JsonValue], extras, _path(path, "extra")
        )
        return cls(**kwargs)

    def to_dict(self) -> dict[str, JsonValue]:
        self._validate_model()
        hints = _schema_hints(type(self))
        result = _coerce_value(dict[str, JsonValue], self.extra, "extra")

        for model_field in fields(self):
            if model_field.name == "extra":
                continue
            value = getattr(self, model_field.name)
            expected = hints[model_field.name]
            converted = _coerce_value(expected, value, model_field.name)
            if converted is None:
                continue
            json_name = model_field.metadata.get("json_name", model_field.name)
            result[json_name] = _encode_value(expected, converted, json_name)
        return result


@dataclass(slots=True)
class ApplicationInfo(JsonModel):
    app_id: str = _json_field("appID")
    interface_revision: int | str = _json_field("interfaceRevision")
    operating_system: str | None = _json_field("operatingSystem", default=None)
    name: str | None = None
    version: str | None = None
    build: int | None = None


@dataclass(slots=True)
class IdentifiedObject(JsonModel):
    id: str


@dataclass(slots=True)
class ImageInfo(JsonModel):
    img_data: str | None = _json_field("imgData", default=None)
    is_app_icon: bool | None = _json_field("isAppIcon", default=None)
    name: str | None = None


@dataclass(slots=True)
class Application(IdentifiedObject):
    name: str | None = None


@dataclass(slots=True)
class Effect(IdentifiedObject):
    name: str | None = None
    is_enabled: bool | None = _json_field("isEnabled", default=None)
    is_supported: bool | None = _json_field("isSupported", default=None)


@dataclass(slots=True)
class ChannelMix(JsonModel):
    id: str | None = None
    mix_id: str | None = _json_field("mixId", default=None)
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)

    def _validate_model(self) -> None:
        if self.id is None and self.mix_id is None:
            raise WaveLinkSchemaError("ChannelMix requires id or mixId")

    @property
    def identifier(self) -> str:
        """Return the mix identifier for either known Wave Link wire shape."""
        if self.id is not None:
            return self.id
        if self.mix_id is not None:
            return self.mix_id
        raise WaveLinkSchemaError("ChannelMix requires id or mixId")


@dataclass(slots=True)
class Channel(IdentifiedObject):
    name: str | None = None
    type: str | None = None
    mixes: list[ChannelMix] | None = None
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    apps: list[Application] | None = None
    effects: list[Effect] | None = None
    image: ImageInfo | None = None


@dataclass(slots=True)
class Mix(IdentifiedObject):
    name: str | None = None
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    image: ImageInfo | None = None


@dataclass(slots=True)
class LevelValue(JsonModel):
    value: float
    min: float | None = None
    max: float | None = None
    look_up_table: list[JsonValue] | None = _json_field("lookUpTable", default=None)
    is_inverted: bool | None = _json_field("isInverted", default=None)


@dataclass(slots=True)
class Input(IdentifiedObject):
    name: str | None = None
    gain: LevelValue | None = None
    mic_pc_mix: LevelValue | None = _json_field("micPcMix", default=None)
    is_muted: bool | None = _json_field("isMuted", default=None)
    is_gain_lock_on: bool | None = _json_field("isGainLockOn", default=None)
    effects: list[Effect] | None = None
    dsp_effects: list[Effect] | None = _json_field("dspEffects", default=None)


@dataclass(slots=True)
class InputDevice(IdentifiedObject):
    name: str | None = None
    device_type: str | None = _json_field("deviceType", default=None)
    is_wave_device: bool | None = _json_field("isWaveDevice", default=None)
    inputs: list[Input] | None = None


@dataclass(slots=True)
class Output(IdentifiedObject):
    name: str | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    level: float | None = None
    mix_id: str | None = _json_field("mixId", default=None)


@dataclass(slots=True)
class OutputDevice(IdentifiedObject):
    name: str | None = None
    device_type: str | None = _json_field("deviceType", default=None)
    is_wave_device: bool | None = _json_field("isWaveDevice", default=None)
    outputs: list[Output] | None = None


@dataclass(slots=True)
class MainOutput(JsonModel):
    output_device_id: str = _json_field("outputDeviceId")
    output_id: str = _json_field("outputId")


@dataclass(slots=True)
class OutputDevices(JsonModel):
    main_output: MainOutput = _json_field("mainOutput")
    output_devices: list[OutputDevice] = _json_field("outputDevices")


@dataclass(slots=True)
class EffectUpdate(IdentifiedObject):
    is_enabled: bool | None = _json_field("isEnabled", default=None)


@dataclass(slots=True)
class InputUpdate(IdentifiedObject):
    gain: LevelValue | None = None
    mic_pc_mix: LevelValue | None = _json_field("micPcMix", default=None)
    is_muted: bool | None = _json_field("isMuted", default=None)
    is_gain_lock_on: bool | None = _json_field("isGainLockOn", default=None)
    effects: list[EffectUpdate] | None = None
    dsp_effects: list[EffectUpdate] | None = _json_field("dspEffects", default=None)


@dataclass(slots=True)
class InputDeviceUpdate(IdentifiedObject):
    inputs: list[InputUpdate]


@dataclass(slots=True)
class ChannelMixUpdate(JsonModel):
    id: str | None = None
    mix_id: str | None = _json_field("mixId", default=None)
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)

    def _validate_model(self) -> None:
        if self.id is None and self.mix_id is None:
            raise WaveLinkSchemaError("ChannelMixUpdate requires id or mixId")


@dataclass(slots=True)
class ChannelUpdate(IdentifiedObject):
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    mixes: list[ChannelMixUpdate] | None = None
    effects: list[EffectUpdate] | None = None


@dataclass(slots=True)
class MixUpdate(IdentifiedObject):
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)


@dataclass(slots=True)
class OutputUpdate(IdentifiedObject):
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    mix_id: str | None = _json_field("mixId", default=None)


@dataclass(slots=True)
class OutputDeviceUpdate(IdentifiedObject):
    outputs: list[OutputUpdate] | None = None


@dataclass(slots=True)
class SetOutputDeviceParams(JsonModel):
    main_output: MainOutput | None = _json_field("mainOutput", default=None)
    output_device: OutputDeviceUpdate | None = _json_field("outputDevice", default=None)

    def _validate_model(self) -> None:
        if self.main_output is None and self.output_device is None:
            raise WaveLinkSchemaError(
                "SetOutputDeviceParams requires mainOutput or outputDevice"
            )


OutputDeviceUpdateParams = SetOutputDeviceParams | MainOutput | OutputDeviceUpdate
OutputDeviceUpdateResult = SetOutputDeviceParams | MainOutput | OutputDeviceUpdate


@dataclass(slots=True)
class PluginInfoResult(JsonModel):
    """Successful ``setPluginInfo`` response (normally an empty object)."""


@dataclass(slots=True)
class FocusedAppSubscription(JsonModel):
    is_enabled: bool = _json_field("isEnabled")


LevelMeterType = Literal["input", "output", "channel", "mix"]


@dataclass(slots=True)
class LevelMeterSubscription(JsonModel):
    type: LevelMeterType
    id: str
    is_enabled: bool = _json_field("isEnabled")
    sub_id: str | None = _json_field("subId", default=None)


@dataclass(slots=True)
class SubscriptionUpdate(JsonModel):
    focused_app_changed: FocusedAppSubscription | None = _json_field(
        "focusedAppChanged", default=None
    )
    level_meter_changed: LevelMeterSubscription | None = _json_field(
        "levelMeterChanged", default=None
    )

    def _validate_model(self) -> None:
        if self.focused_app_changed is None and self.level_meter_changed is None:
            raise WaveLinkSchemaError("SubscriptionUpdate requires a subscription")


@dataclass(slots=True)
class FocusedAppChannel(JsonModel):
    id: str


@dataclass(slots=True)
class FocusedAppChanged(IdentifiedObject):
    name: str | None = None
    channel: FocusedAppChannel | None = None


@dataclass(slots=True)
class MeterEntry(IdentifiedObject):
    sub_id: str | None = _json_field("subId", default=None)
    level_left_percentage: float | None = _json_field(
        "levelLeftPercentage", default=None
    )
    level_right_percentage: float | None = _json_field(
        "levelRightPercentage", default=None
    )


@dataclass(slots=True)
class LevelMeterChanged(JsonModel):
    input_devices: list[MeterEntry] | None = _json_field("inputDevices", default=None)
    output_devices: list[MeterEntry] | None = _json_field("outputDevices", default=None)
    channels: list[MeterEntry] | None = None
    mixes: list[MeterEntry] | None = None


@dataclass(slots=True)
class CreateProfileRequested(JsonModel):
    device_type: str | None = _json_field("deviceType", default=None)
    mixes: list[str] | None = None


__all__ = [
    "Application",
    "ApplicationInfo",
    "Channel",
    "ChannelMix",
    "ChannelMixUpdate",
    "ChannelUpdate",
    "Effect",
    "EffectUpdate",
    "CreateProfileRequested",
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
    "LevelMeterSubscription",
    "LevelMeterChanged",
    "LevelMeterType",
    "LevelValue",
    "MeterEntry",
    "MainOutput",
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
    "WaveLinkSchemaError",
]
