"""Typed models for the Wave Link JSON-RPC API.

RPC wrappers return these dataclasses instead of untyped dictionaries. Model
attributes use Python ``snake_case`` names while :meth:`JsonModel.to_dict`
converts them to the JSON field names expected by Wave Link. Conversely,
:meth:`JsonModel.from_dict` validates an incoming object and recursively builds
nested models.

Unknown fields are preserved in :attr:`JsonModel.extra`. This allows callers to
round-trip data added by newer Wave Link versions without silently discarding
it. Optional attributes whose value is ``None`` are omitted when serialized.
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


#: Any scalar value that can be represented by JSON.
JsonScalar: TypeAlias = str | int | float | bool | None
#: A recursively typed JSON value accepted by the Wave Link transport.
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class WaveLinkSchemaError(ValueError):
    """Raised when data does not satisfy a typed Wave Link model.

    The error message includes the failing JSON path whenever validation occurs
    during :meth:`JsonModel.from_dict`. It may also be raised when constructing
    a model directly or serializing a model that violates a cross-field rule.
    """


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
    """Base class for typed Wave Link JSON objects.

    Direct construction validates field types in ``__post_init__``. Use
    :meth:`from_dict` for untrusted RPC data because it also converts nested
    dictionaries into their declared model types.

    Attributes:
        extra: Unknown JSON fields retained for forward-compatible round trips.

    Raises:
        WaveLinkSchemaError: If a field has an invalid type or value.
    """

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
        """Validate a JSON object and convert it into this model class.

        Nested model fields and collections are converted recursively. Unknown
        keys are copied into :attr:`extra`, while missing optional fields keep
        their dataclass defaults.

        Args:
            value: Mapping containing the JSON object returned by Wave Link.
            path: Optional parent path used to produce precise validation errors.

        Returns:
            A validated instance of the concrete model class.

        Raises:
            WaveLinkSchemaError: If ``value`` is not an object, a required field
                is missing, or any field fails validation.
        """
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
        """Serialize the model to the JSON shape used by Wave Link.

        Python attribute names are mapped back to their JSON names, nested
        models are serialized recursively, ``None`` values are omitted, and
        fields stored in :attr:`extra` are preserved.

        Returns:
            A new JSON-compatible dictionary suitable for an RPC payload.

        Raises:
            WaveLinkSchemaError: If the model was mutated into an invalid state.
        """
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
    """Metadata reported by ``getApplicationInfo``.

    Attributes:
        app_id: Application identifier. A Wave Link server normally reports
            ``"EWL"``.
        interface_revision: Revision of the local JSON-RPC interface.
        operating_system: Operating-system name reported by Wave Link, if any.
        name: Human-readable application name.
        version: Wave Link release version.
        build: Numeric application build, when provided.
    """

    app_id: str = _json_field("appID")
    interface_revision: int | str = _json_field("interfaceRevision")
    operating_system: str | None = _json_field("operatingSystem", default=None)
    name: str | None = None
    version: str | None = None
    build: int | None = None


@dataclass(slots=True)
class IdentifiedObject(JsonModel):
    """Base model for Wave Link objects addressed by an identifier.

    Attributes:
        id: Identifier used in subsequent RPC calls and notifications.
    """

    id: str


@dataclass(slots=True)
class ImageInfo(JsonModel):
    """Image metadata attached to a channel or mix.

    Attributes:
        img_data: Image payload supplied by Wave Link.
        is_app_icon: Whether the image represents an application icon.
        name: Human-readable image name.
    """

    img_data: str | None = _json_field("imgData", default=None)
    is_app_icon: bool | None = _json_field("isAppIcon", default=None)
    name: str | None = None


@dataclass(slots=True)
class Application(IdentifiedObject):
    """Application currently assigned to a software channel.

    Attributes:
        id: Application identifier accepted by :meth:`WaveLinkClient.add_to_channel`.
        name: Human-readable application name.
    """

    name: str | None = None


@dataclass(slots=True)
class Effect(IdentifiedObject):
    """Software or hardware effect and its current state.

    Attributes:
        id: Effect identifier used by effect setter methods.
        name: Human-readable effect name.
        is_enabled: Whether the effect is currently active.
        is_supported: Whether the connected device supports the effect.
    """

    name: str | None = None
    is_enabled: bool | None = _json_field("isEnabled", default=None)
    is_supported: bool | None = _json_field("isSupported", default=None)


@dataclass(slots=True)
class ChannelMix(JsonModel):
    """Per-mix state of a channel.

    Wave Link versions use either ``id`` or ``mixId`` for the mix identifier.
    The model accepts both forms and exposes a normalized :attr:`identifier`.

    Attributes:
        id: Mix identifier used by current known response shapes.
        mix_id: Mix identifier used by the alternative documented shape.
        level: Channel level within this mix, normally between ``0.0`` and ``1.0``.
        is_muted: Whether the channel is muted in this mix.

    Raises:
        WaveLinkSchemaError: If neither identifier field is provided.
    """

    id: str | None = None
    mix_id: str | None = _json_field("mixId", default=None)
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)

    def _validate_model(self) -> None:
        if self.id is None and self.mix_id is None:
            raise WaveLinkSchemaError("ChannelMix requires id or mixId")

    @property
    def identifier(self) -> str:
        """Return the mix identifier from either supported wire shape.

        Returns:
            :attr:`id` when available, otherwise :attr:`mix_id`.

        Raises:
            WaveLinkSchemaError: If both identifier fields are missing.
        """
        if self.id is not None:
            return self.id
        if self.mix_id is not None:
            return self.mix_id
        raise WaveLinkSchemaError("ChannelMix requires id or mixId")


@dataclass(slots=True)
class Channel(IdentifiedObject):
    """Mixer channel returned by ``getChannels`` and channel events.

    Attributes:
        id: Channel identifier used by channel setter methods.
        name: Human-readable channel name.
        type: Wave Link channel type.
        mixes: Per-mix levels and mute states for the channel.
        level: Global channel level, normally between ``0.0`` and ``1.0``.
        is_muted: Global channel mute state.
        apps: Applications currently routed to the channel.
        effects: Effects attached to the channel.
        image: Optional channel image metadata.
    """

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
    """Wave Link mix and its current master state.

    Attributes:
        id: Mix identifier used by mix setter and subscription methods.
        name: Human-readable mix name.
        level: Master mix level, normally between ``0.0`` and ``1.0``.
        is_muted: Whether the mix is muted.
        image: Optional mix image metadata.
    """

    name: str | None = None
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    image: ImageInfo | None = None


@dataclass(slots=True)
class LevelValue(JsonModel):
    """Numeric value with optional range and presentation metadata.

    Attributes:
        value: Current numeric value.
        min: Minimum value advertised by Wave Link.
        max: Maximum value advertised by Wave Link.
        look_up_table: Optional display-value lookup table.
        is_inverted: Whether the visual or hardware scale is inverted.
    """

    value: float
    min: float | None = None
    max: float | None = None
    look_up_table: list[JsonValue] | None = _json_field("lookUpTable", default=None)
    is_inverted: bool | None = _json_field("isInverted", default=None)


@dataclass(slots=True)
class Input(IdentifiedObject):
    """Physical or virtual input exposed by an input device.

    Attributes:
        id: Input identifier used together with the parent device identifier.
        name: Human-readable input name.
        gain: Input gain value and associated metadata.
        mic_pc_mix: Microphone/PC balance value, when supported.
        is_muted: Input mute state.
        is_gain_lock_on: Hardware gain-lock state.
        effects: Software effects available for the input.
        dsp_effects: Hardware or DSP effects available for the input.
    """

    name: str | None = None
    gain: LevelValue | None = None
    mic_pc_mix: LevelValue | None = _json_field("micPcMix", default=None)
    is_muted: bool | None = _json_field("isMuted", default=None)
    is_gain_lock_on: bool | None = _json_field("isGainLockOn", default=None)
    effects: list[Effect] | None = None
    dsp_effects: list[Effect] | None = _json_field("dspEffects", default=None)


@dataclass(slots=True)
class InputDevice(IdentifiedObject):
    """Device containing one or more controllable inputs.

    Attributes:
        id: Device identifier used by input setter methods.
        name: Human-readable device name.
        device_type: Device type reported by Wave Link.
        is_wave_device: Whether this is an Elgato Wave-family device.
        inputs: Inputs belonging to the device.
    """

    name: str | None = None
    device_type: str | None = _json_field("deviceType", default=None)
    is_wave_device: bool | None = _json_field("isWaveDevice", default=None)
    inputs: list[Input] | None = None


@dataclass(slots=True)
class Output(IdentifiedObject):
    """Physical or virtual output exposed by an output device.

    Attributes:
        id: Output identifier used together with the parent device identifier.
        name: Human-readable output name.
        is_muted: Output mute state.
        level: Output level, normally between ``0.0`` and ``1.0``.
        mix_id: Identifier of the mix currently routed to the output.
    """

    name: str | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    level: float | None = None
    mix_id: str | None = _json_field("mixId", default=None)


@dataclass(slots=True)
class OutputDevice(IdentifiedObject):
    """Device containing one or more controllable outputs.

    Attributes:
        id: Device identifier used by output setter methods.
        name: Human-readable device name.
        device_type: Device type reported by Wave Link.
        is_wave_device: Whether this is an Elgato Wave-family device.
        outputs: Outputs belonging to the device.
    """

    name: str | None = None
    device_type: str | None = _json_field("deviceType", default=None)
    is_wave_device: bool | None = _json_field("isWaveDevice", default=None)
    outputs: list[Output] | None = None


@dataclass(slots=True)
class MainOutput(JsonModel):
    """Reference to the output selected as Wave Link's main output.

    Attributes:
        output_device_id: Identifier of the containing output device.
        output_id: Identifier of the selected output. An empty string clears it
            on Wave Link versions that support clearing the main output.
    """

    output_device_id: str = _json_field("outputDeviceId")
    output_id: str = _json_field("outputId")


@dataclass(slots=True)
class OutputDevices(JsonModel):
    """Envelope returned by ``getOutputDevices``.

    Attributes:
        main_output: Currently selected main-output reference.
        output_devices: Available devices and their outputs.
    """

    main_output: MainOutput = _json_field("mainOutput")
    output_devices: list[OutputDevice] = _json_field("outputDevices")


@dataclass(slots=True)
class EffectUpdate(IdentifiedObject):
    """Partial update for an effect.

    Attributes:
        id: Effect identifier.
        is_enabled: Desired enabled state. ``None`` leaves the field unchanged.
    """

    is_enabled: bool | None = _json_field("isEnabled", default=None)


@dataclass(slots=True)
class InputUpdate(IdentifiedObject):
    """Partial update for one input.

    Only non-``None`` fields are emitted in an RPC payload.

    Attributes:
        id: Input identifier.
        gain: Desired gain value.
        mic_pc_mix: Desired microphone/PC balance.
        is_muted: Desired mute state.
        is_gain_lock_on: Desired hardware gain-lock state.
        effects: Software-effect updates.
        dsp_effects: Hardware or DSP-effect updates.
    """

    gain: LevelValue | None = None
    mic_pc_mix: LevelValue | None = _json_field("micPcMix", default=None)
    is_muted: bool | None = _json_field("isMuted", default=None)
    is_gain_lock_on: bool | None = _json_field("isGainLockOn", default=None)
    effects: list[EffectUpdate] | None = None
    dsp_effects: list[EffectUpdate] | None = _json_field("dspEffects", default=None)


@dataclass(slots=True)
class InputDeviceUpdate(IdentifiedObject):
    """Update payload and response for ``setInputDevice``.

    Attributes:
        id: Input-device identifier.
        inputs: One or more input updates applied as a single RPC operation.
    """

    inputs: list[InputUpdate]


@dataclass(slots=True)
class ChannelMixUpdate(JsonModel):
    """Partial update for a channel inside one mix.

    Wave Link versions accept either ``id`` or ``mixId`` for the target mix.

    Attributes:
        id: Mix identifier in the currently observed payload shape.
        mix_id: Mix identifier in the alternative documented payload shape.
        level: Desired per-mix channel level.
        is_muted: Desired per-mix mute state.

    Raises:
        WaveLinkSchemaError: If neither identifier field is provided.
    """

    id: str | None = None
    mix_id: str | None = _json_field("mixId", default=None)
    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)

    def _validate_model(self) -> None:
        if self.id is None and self.mix_id is None:
            raise WaveLinkSchemaError("ChannelMixUpdate requires id or mixId")


@dataclass(slots=True)
class ChannelUpdate(IdentifiedObject):
    """Partial update for ``setChannel``.

    Attributes:
        id: Channel identifier.
        level: Desired global channel level.
        is_muted: Desired global mute state.
        mixes: Per-mix updates.
        effects: Channel-effect updates.
    """

    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    mixes: list[ChannelMixUpdate] | None = None
    effects: list[EffectUpdate] | None = None


@dataclass(slots=True)
class MixUpdate(IdentifiedObject):
    """Partial update for ``setMix``.

    Attributes:
        id: Mix identifier.
        level: Desired master level.
        is_muted: Desired mute state.
    """

    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)


@dataclass(slots=True)
class OutputUpdate(IdentifiedObject):
    """Partial update for one output.

    Attributes:
        id: Output identifier.
        level: Desired output level.
        is_muted: Desired output mute state.
        mix_id: Mix to route to the output. An empty string removes routing.
    """

    level: float | None = None
    is_muted: bool | None = _json_field("isMuted", default=None)
    mix_id: str | None = _json_field("mixId", default=None)


@dataclass(slots=True)
class OutputDeviceUpdate(IdentifiedObject):
    """Partial update for an output device.

    Attributes:
        id: Output-device identifier.
        outputs: Output updates belonging to this device.
    """

    outputs: list[OutputUpdate] | None = None


@dataclass(slots=True)
class SetOutputDeviceParams(JsonModel):
    """Documented parameter envelope for ``setOutputDevice``.

    At least one of :attr:`main_output` or :attr:`output_device` is required.

    Attributes:
        main_output: Optional main-output selection update.
        output_device: Optional update for one output device.

    Raises:
        WaveLinkSchemaError: If both update fields are omitted.
    """

    main_output: MainOutput | None = _json_field("mainOutput", default=None)
    output_device: OutputDeviceUpdate | None = _json_field("outputDevice", default=None)

    def _validate_model(self) -> None:
        if self.main_output is None and self.output_device is None:
            raise WaveLinkSchemaError(
                "SetOutputDeviceParams requires mainOutput or outputDevice"
            )


#: Parameter shapes accepted by :meth:`WaveLinkClient.set_output_device`.
OutputDeviceUpdateParams = SetOutputDeviceParams | MainOutput | OutputDeviceUpdate
#: Response shapes observed from the ``setOutputDevice`` RPC method.
OutputDeviceUpdateResult = SetOutputDeviceParams | MainOutput | OutputDeviceUpdate


@dataclass(slots=True)
class PluginInfoResult(JsonModel):
    """Successful ``setPluginInfo`` response, normally an empty object."""


@dataclass(slots=True)
class FocusedAppSubscription(JsonModel):
    """Enable or disable focused-application notifications.

    Attributes:
        is_enabled: Desired subscription state.
    """

    is_enabled: bool = _json_field("isEnabled")


#: Kinds of Wave Link objects that can publish real-time level meters.
LevelMeterType = Literal["input", "output", "channel", "mix"]


@dataclass(slots=True)
class LevelMeterSubscription(JsonModel):
    """Subscription request for one real-time level meter.

    Attributes:
        type: Target category: ``input``, ``output``, ``channel``, or ``mix``.
        id: Identifier of the target object.
        is_enabled: Whether to enable or disable the subscription.
        sub_id: Optional caller-defined subscription identifier echoed in events.
    """

    type: LevelMeterType
    id: str
    is_enabled: bool = _json_field("isEnabled")
    sub_id: str | None = _json_field("subId", default=None)


@dataclass(slots=True)
class SubscriptionUpdate(JsonModel):
    """Parameter and response model for ``setSubscription``.

    At least one subscription field must be supplied.

    Attributes:
        focused_app_changed: Focused-application subscription update.
        level_meter_changed: Level-meter subscription update.

    Raises:
        WaveLinkSchemaError: If no subscription update is provided.
    """

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
    """Channel associated with the currently focused application.

    Attributes:
        id: Channel identifier.
    """

    id: str


@dataclass(slots=True)
class FocusedAppChanged(IdentifiedObject):
    """Typed ``focusedAppChanged`` notification.

    Attributes:
        id: Focused application identifier.
        name: Human-readable application name.
        channel: Channel to which the application is currently assigned.
    """

    name: str | None = None
    channel: FocusedAppChannel | None = None


@dataclass(slots=True)
class MeterEntry(IdentifiedObject):
    """Stereo level-meter sample for one subscribed object.

    Attributes:
        id: Identifier of the measured input, output, channel, or mix.
        sub_id: Optional subscription identifier from the request.
        level_left_percentage: Left-channel level percentage.
        level_right_percentage: Right-channel level percentage.
    """

    sub_id: str | None = _json_field("subId", default=None)
    level_left_percentage: float | None = _json_field(
        "levelLeftPercentage", default=None
    )
    level_right_percentage: float | None = _json_field(
        "levelRightPercentage", default=None
    )


@dataclass(slots=True)
class LevelMeterChanged(JsonModel):
    """Typed ``levelMeterChanged`` notification.

    Each optional collection groups samples by target category. Wave Link may
    include one or several collections in a notification.

    Attributes:
        input_devices: Meter samples for subscribed inputs.
        output_devices: Meter samples for subscribed outputs.
        channels: Meter samples for subscribed channels.
        mixes: Meter samples for subscribed mixes.
    """

    input_devices: list[MeterEntry] | None = _json_field("inputDevices", default=None)
    output_devices: list[MeterEntry] | None = _json_field("outputDevices", default=None)
    channels: list[MeterEntry] | None = None
    mixes: list[MeterEntry] | None = None


@dataclass(slots=True)
class CreateProfileRequested(JsonModel):
    """Typed ``createProfileRequested`` notification.

    Attributes:
        device_type: Device family for which a profile was requested.
        mixes: Mix identifiers associated with the requested profile.
    """

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
