"""Статические схемы JSON-RPC API Wave Link.

API версионируется независимо от клиента и со временем может получать новые
поля. Поэтому схемы ответов требуют только стабильные поля идентификации и
обёртки, одновременно описывая поля, обнаруженные в Wave Link 3.2.5 (ревизия
интерфейса 2). Благодаря ``TypedDict`` публичное представление во время
выполнения остаётся обычными словарями.
"""

from __future__ import annotations

from typing import Literal, Required, TypeAlias, TypedDict


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class ApplicationInfo(TypedDict, total=False):
    appID: Required[str]
    interfaceRevision: Required[int | str]
    operatingSystem: str
    name: str
    version: str
    build: int


class IdentifiedObject(TypedDict):
    id: str


class ImageInfo(TypedDict, total=False):
    """Встроенное изображение канала или именованное системное изображение микса."""

    imgData: str
    isAppIcon: bool
    name: str


class Application(IdentifiedObject, total=False):
    name: str


class Effect(IdentifiedObject, total=False):
    name: str
    isEnabled: bool


class ChannelMix(IdentifiedObject, total=False):
    level: float
    isMuted: bool


class Channel(IdentifiedObject, total=False):
    name: str
    type: str
    mixes: list[ChannelMix]
    level: float
    isMuted: bool
    apps: list[Application]
    effects: list[Effect]
    image: ImageInfo


class Mix(IdentifiedObject, total=False):
    name: str
    level: float
    isMuted: bool
    image: ImageInfo


class LevelValue(TypedDict, total=False):
    value: Required[float]
    min: float
    max: float
    # Для сторонних устройств таблица пуста; структура её заполненных элементов
    # зависит от подключённого оборудования Elgato.
    lookUpTable: list[object]


class Input(IdentifiedObject, total=False):
    name: str
    gain: LevelValue
    micPcMix: LevelValue
    isMuted: bool
    effects: list[Effect]
    dspEffects: list[Effect]


class InputDevice(IdentifiedObject, total=False):
    name: str
    deviceType: str
    inputs: list[Input]


class Output(IdentifiedObject, total=False):
    name: str
    isMuted: bool
    level: float
    mixId: str


class OutputDevice(IdentifiedObject, total=False):
    name: str
    deviceType: str
    outputs: list[Output]


class MainOutput(TypedDict):
    outputDeviceId: str
    outputId: str


class OutputDevices(TypedDict):
    mainOutput: MainOutput
    outputDevices: list[OutputDevice]


# Структуры изменяющих запросов намеренно отделены от полных объектов ответов:
# setter-методы Wave Link принимают частичные обновления, но каждая изменяемая
# сущность всё равно должна иметь идентификатор.


class EffectUpdate(IdentifiedObject, total=False):
    isEnabled: bool


class InputUpdate(IdentifiedObject, total=False):
    gain: LevelValue
    micPcMix: LevelValue
    isMuted: bool
    effects: list[EffectUpdate]
    dspEffects: list[EffectUpdate]


class InputDeviceUpdate(IdentifiedObject, total=False):
    inputs: list[InputUpdate]


class ChannelMixUpdate(TypedDict, total=False):
    # Текущие сборки используют ``id``, некоторым старым требуется ``mixId``.
    id: str
    mixId: str
    level: float
    isMuted: bool


class ChannelUpdate(IdentifiedObject, total=False):
    level: float
    isMuted: bool
    mixes: list[ChannelMixUpdate]
    effects: list[EffectUpdate]


class MixUpdate(IdentifiedObject, total=False):
    level: float
    isMuted: bool


class OutputUpdate(IdentifiedObject, total=False):
    level: float
    isMuted: bool
    mixId: str


class OutputDeviceUpdate(IdentifiedObject, total=False):
    outputs: list[OutputUpdate]


class SetOutputDeviceParams(TypedDict, total=False):
    mainOutput: MainOutput
    outputDevice: OutputDeviceUpdate


# Плоские варианты сохранены для сборок Wave Link, которые отклоняют
# документированную вложенную структуру параметров setOutputDevice.
OutputDeviceUpdateParams = SetOutputDeviceParams | MainOutput | OutputDeviceUpdate
OutputDeviceUpdateResult = SetOutputDeviceParams | MainOutput | OutputDeviceUpdate


class PluginInfoResult(TypedDict):
    """Успешный ответ setPluginInfo — пустой JSON-объект."""


class FocusedAppSubscription(TypedDict):
    isEnabled: bool


LevelMeterType = Literal["input", "output", "channel", "mix"]


class LevelMeterSubscription(TypedDict, total=False):
    type: Required[LevelMeterType]
    id: Required[str]
    isEnabled: Required[bool]
    subId: str


class SubscriptionUpdate(TypedDict, total=False):
    focusedAppChanged: FocusedAppSubscription
    levelMeterChanged: LevelMeterSubscription


__all__ = [
    "Application",
    "ApplicationInfo",
    "Channel",
    "ChannelMix",
    "ChannelMixUpdate",
    "ChannelUpdate",
    "Effect",
    "EffectUpdate",
    "FocusedAppSubscription",
    "ImageInfo",
    "Input",
    "InputDevice",
    "InputDeviceUpdate",
    "InputUpdate",
    "JsonScalar",
    "JsonValue",
    "LevelMeterSubscription",
    "LevelMeterType",
    "LevelValue",
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
]
