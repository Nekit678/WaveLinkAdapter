# WaveLinkAdapter

`WaveLinkAdapter` — асинхронное Python-ядро для локального WebSocket/JSON-RPC
API Elgato Wave Link 3.x. Оно подключается к запущенному Wave Link, читает
текущее состояние аудиоканалов и позволяет управлять входами, выходами,
миксами, эффектами и подписками на события. Его можно использовать как основу для REST API,
WebSocket-шлюза, интеграции со Stream Deck, настольного приложения или сценариев автоматизации.

## Возможности

- автоматический поиск WebSocket-порта Wave Link;
- работа непосредственно в Windows и из WSL;
- асинхронный JSON-RPC-клиент;
- одновременное выполнение нескольких RPC-запросов;
- таймауты и отдельные типы ошибок;
- автоматическое переподключение с экспоненциальной задержкой;
- восстановление метаданных плагина и подписок после переподключения;
- получение каналов, миксов, входных и выходных устройств;
- управление громкостью, mute, маршрутизацией и эффектами;
- синхронные и асинхронные обработчики событий;
- объектные `dataclass`-схемы с проверкой типов и вложенных структур;
- низкоуровневый `call()` для методов без готовой обёртки.

Клиент проверен с Elgato Wave Link `3.2.5.3731`, interface revision `2`.

## Требования

- Python 3.11 или новее;
- Elgato Wave Link 3.x, запущенный на Windows;
- пакет `websockets>=16,<17`.

Python 3.11 требуется из-за использования `asyncio.timeout()`.

## Установка

Из каталога проекта:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Linux или WSL:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Перед подключением запустите Wave Link. При работе из WSL приложение Wave Link
продолжает работать в Windows, а адаптер автоматически ищет его файл
`ws-info.json` на смонтированных Windows-дисках.

## Быстрый старт

```python
import asyncio

from wavelink_core import WaveLinkClient


async def main() -> None:
    async with WaveLinkClient() as client:
        info = await client.get_application_info()
        print("Wave Link:", info.version)

        channels = await client.get_channels()
        for channel in channels:
            print(channel.id, channel.name)


asyncio.run(main())
```

Контекстный менеджер вызывает `connect()` при входе и `close()` при выходе,
в том числе при исключении. Это рекомендуемый способ использования клиента в
коротких сценариях.

## Объектные схемы

Высокоуровневые методы возвращают `dataclass`-объекты из `wavelink_types`, а
не исходные JSON-словари. Поля доступны через Python-атрибуты в `snake_case`,
вложенные объекты также преобразуются в соответствующие модели:

```python
channels = await client.get_channels()
channel = channels[0]

print(channel.id, channel.name, channel.is_muted)
for mix in channel.mixes or []:
    print(mix.id, mix.level)
```

Каждая схема предоставляет `from_dict()` и `to_dict()`:

```python
from wavelink_types import ChannelUpdate


update = ChannelUpdate(id="channel-id", level=0.5, is_muted=False)
print(update.to_dict())
# {'id': 'channel-id', 'level': 0.5, 'isMuted': False}
```

При разборе проверяются обязательные поля, типы и вложенные структуры.
Неизвестные поля новой версии Wave Link сохраняются в атрибуте `extra` и
возвращаются при `to_dict()`. Низкоуровневый `call()` по-прежнему принимает и
возвращает обычные JSON-значения.

Setter-методы принимают только объектные update-схемы. Исходные JSON-словари
используются исключительно на уровне `call()` и внутри транспорта.

## Долгоживущий клиент

В серверном приложении следует создать один экземпляр `WaveLinkClient` и
использовать его всё время работы процесса. Не создавайте новое WebSocket-
соединение для каждого HTTP-запроса.

```python
from wavelink_core import WaveLinkClient


client = WaveLinkClient(
    host="127.0.0.1",
    rpc_timeout=5.0,
    auto_reconnect=True,
)


async def application_startup() -> None:
    await client.connect()


async def application_shutdown() -> None:
    await client.close()
```

После потери уже установленного соединения клиент автоматически пытается
подключиться снова. Новый RPC-вызов во время реконнекта ждёт восстановления
соединения не дольше своего таймаута.

## Настройка клиента

```python
client = WaveLinkClient(
    host="127.0.0.1",
    debug=False,
    rpc_timeout=10.0,
    open_timeout=3.0,
    close_timeout=3.0,
    event_queue_size=256,
    auto_reconnect=True,
    reconnect_initial_delay=0.5,
    reconnect_max_delay=10.0,
    reconnect_backoff=2.0,
)
```

| Параметр | Назначение |
| --- | --- |
| `host` | Хост, на котором доступен Wave Link. По умолчанию `127.0.0.1`. |
| `debug` | Вывод исходящих и входящих WebSocket-сообщений через `logging`. |
| `rpc_timeout` | Таймаут RPC в секундах. `None` отключает таймаут. |
| `open_timeout` | Таймаут открытия одного WebSocket-соединения. |
| `close_timeout` | Таймаут закрытия WebSocket-соединения. |
| `event_queue_size` | Максимальное количество ожидающих обработки событий. |
| `auto_reconnect` | Включает реконнект после потери установленного соединения. |
| `reconnect_initial_delay` | Начальная задержка между попытками реконнекта. |
| `reconnect_max_delay` | Максимальная задержка между попытками. |
| `reconnect_backoff` | Множитель задержки после каждой неудачной попытки. |

Для просмотра отладочного обмена:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
client = WaveLinkClient(debug=True)
```

## Соединение и поиск порта

`discover_ports()` синхронно возвращает порты, которые клиент проверит при
подключении. Сначала идут порты из найденных файлов `ws-info.json`, затем
резервный диапазон Wave Link 3.x:

```python
client = WaveLinkClient()
print(client.discover_ports())
```

Обычный жизненный цикл без контекстного менеджера:

```python
client = WaveLinkClient()

try:
    await client.connect()
    await client.wait_until_connected(timeout=5.0)
    channels = await client.get_channels()
finally:
    await client.close()
```

`connect()` безопасно вызывать повторно на уже подключённом клиенте. Метод
`wait_until_connected()` немедленно завершается при активном соединении либо
ждёт окончания автоматического реконнекта. При истечении переданного таймаута
возникает `WaveLinkTimeoutError`.

## Поиск устройств и идентификаторов

Изменяющие методы принимают идентификаторы, возвращённые Wave Link. Не следует
сохранять примерные ID в коде: получите актуальное состояние после подключения.

```python
async with WaveLinkClient() as client:
    channels = await client.get_channels()
    mixes = await client.get_mixes()
    input_devices = await client.get_input_devices()
    output_state = await client.get_output_devices()

    for channel in channels:
        print("channel", channel.id, channel.name)

    for mix in mixes:
        print("mix", mix.id, mix.name)

    for device in input_devices:
        print("input device", device.id, device.name)
        for input_ in device.inputs or []:
            print("  input", input_.id, input_.name)

    for device in output_state.output_devices:
        print("output device", device.id, device.name)
        for output in device.outputs or []:
            print("  output", output.id, output.name)
```

## Уровни громкости

Уровни задаются числами от `0.0` до `1.0`:

- `0.0` — минимальный уровень;
- `0.5` — половина диапазона API;
- `1.0` — максимальный уровень.

Методы удобного доступа автоматически ограничивают значение этим диапазоном.
Например, `1.5` преобразуется в `1.0`, а `-0.2` — в `0.0`. Значения `NaN`,
бесконечность и `bool` отклоняются.

## Чтение состояния

### `get_application_info()`

Возвращает информацию о приложении и версии интерфейса:

```python
info = await client.get_application_info()
print(info.app_id)
print(info.interface_revision)
print(info.version)
```

### `get_channels()`

Возвращает список каналов. Каждый канал обязательно содержит `id` и может
содержать имя, уровень, mute, приложения, эффекты и настройки отдельных миксов.

```python
channels = await client.get_channels()
```

### `get_mixes()`

Возвращает доступные мониторные и потоковые миксы:

```python
mixes = await client.get_mixes()
```

### `get_input_devices()`

Возвращает входные устройства и принадлежащие им входы:

```python
devices = await client.get_input_devices()
```

### `get_output_devices()`

Возвращает объект с двумя полями:

- `mainOutput` — выбранное главное устройство и выход;
- `outputDevices` — список доступных выходных устройств.

```python
state = await client.get_output_devices()
print(state.main_output.output_device_id, state.main_output.output_id)
```

## Управление входами

```python
await client.set_input_mute(device_id, input_id, True)
await client.set_input_gain(device_id, input_id, 0.65)
await client.set_input_gain_lock(device_id, input_id, True)
await client.set_input_mic_pc_mix(device_id, input_id, 0.5)
```

Включение обычного программного эффекта:

```python
await client.set_input_effect_enabled(
    device_id,
    input_id,
    effect_id,
    True,
)
```

Включение аппаратного/DSP-эффекта:

```python
await client.set_input_effect_enabled(
    device_id,
    input_id,
    effect_id,
    True,
    dsp=True,
)
```

Несколько свойств входа можно изменить одним запросом:

```python
from wavelink_types import InputUpdate, LevelValue


await client.set_input_device(
    device_id,
    [
        InputUpdate(
            id=input_id,
            is_muted=False,
            gain=LevelValue(value=0.7),
        )
    ],
)
```

## Управление выходами

```python
await client.set_output_level(output_device_id, output_id, 0.8)
await client.set_output_mute(output_device_id, output_id, False)
await client.set_output_mix(output_device_id, output_id, mix_id)
await client.remove_output_from_mix(output_device_id, output_id)
```

Назначение главного выхода:

```python
await client.set_main_output(output_device_id, output_id)
```

Низкоуровневое частичное обновление:

```python
from wavelink_types import (
    OutputDeviceUpdate,
    OutputUpdate,
    SetOutputDeviceParams,
)


await client.set_output_device(
    SetOutputDeviceParams(
        output_device=OutputDeviceUpdate(
            id=output_device_id,
            outputs=[
                OutputUpdate(
                    id=output_id,
                    level=0.8,
                    is_muted=False,
                )
            ],
        )
    )
)
```

Клиент поддерживает как документированную вложенную форму
`setOutputDevice`, так и известные плоские варианты старых сборок. Повторный
запрос в совместимой форме выполняется только при RPC-ошибке `-32602`.

## Управление каналами

```python
await client.set_channel_level(channel_id, 0.5)
await client.set_channel_mute(channel_id, True)
await client.set_channel_mix_level(channel_id, mix_id, 0.75)
await client.set_channel_mix_mute(channel_id, mix_id, False)
await client.set_channel_effect_enabled(channel_id, effect_id, True)
```

Произвольное частичное обновление канала:

```python
from wavelink_types import ChannelUpdate


await client.set_channel(
    ChannelUpdate(
        id=channel_id,
        level=0.5,
        is_muted=False,
    )
)
```

Назначение приложения программному каналу:

```python
await client.add_to_channel(application_id, channel_id)
```

Для `set_channel_mix_level()` и `set_channel_mix_mute()` автоматически
поддерживаются обе известные формы идентификатора микса: `id` и `mixId`.

## Управление миксами

```python
await client.set_mix_level(mix_id, 0.9)
await client.set_mix_mute(mix_id, False)
```

Произвольное частичное обновление:

```python
from wavelink_types import MixUpdate


await client.set_mix(
    MixUpdate(
        id=mix_id,
        level=0.9,
        is_muted=False,
    )
)
```

## События и подписки

Обработчик регистрируется методом `on()`:

```python
def on_focused_app(params: dict) -> None:
    print("Focused application:", params)


client.on("focusedAppChanged", on_focused_app)
await client.subscribe_focused_app(True)
```

Поддерживаются и асинхронные обработчики:

```python
async def on_level(params: dict) -> None:
    await send_to_browser(params)


client.on("levelMeterChanged", on_level)
await client.subscribe_level_meter("channel", channel_id)
```

Допустимые типы level meter:

- `input`;
- `output`;
- `channel`;
- `mix`.

Подписка с пользовательским `subId`:

```python
await client.subscribe_level_meter(
    "output",
    output_device_id,
    sub_id="headphones",
)
```

Отключение подписки выполняется тем же методом с `enabled=False`:

```python
await client.subscribe_level_meter(
    "channel",
    channel_id,
    False,
)
```

Вспомогательные методы:

```python
await client.subscribe_realtime()
await client.try_subscribe_level_meters()
```

Несколько категорий можно обновить одним низкоуровневым вызовом
`set_subscription()`:

```python
from wavelink_types import (
    FocusedAppSubscription,
    LevelMeterSubscription,
    SubscriptionUpdate,
)


await client.set_subscription(
    SubscriptionUpdate(
        focused_app_changed=FocusedAppSubscription(is_enabled=True),
        level_meter_changed=LevelMeterSubscription(
            type="channel",
            id=channel_id,
            is_enabled=True,
        ),
    )
)
```

`try_subscribe_level_meters()` получает актуальные входы, выходы, каналы и
миксы, после чего подписывается на каждый конкретный идентификатор. Wave Link
3.2.5 не принимает псевдоидентификатор `all`, поэтому количество запросов
зависит от текущей конфигурации микшера. Каждая успешно включённая
meter-подписка хранится отдельно и восстанавливается после переподключения;
отключённая подписка удаляется из реестра восстановления.

Удаление обработчика:

```python
removed = client.off("focusedAppChanged", on_focused_app)
```

`off()` возвращает `True`, если регистрация была найдена и удалена.

Для известных уведомлений доступен типизированный вариант `on_typed()`.
Обработчик получает проверенную модель из `wavelink_types`, а не словарь:

```python
from wavelink_types import FocusedAppChanged


def on_focused_app(event: FocusedAppChanged) -> None:
    print(event.name, event.channel.id if event.channel else None)


client.on_typed("focusedAppChanged", on_focused_app)
```

Те же события можно читать как асинхронный поток. Внутренняя очередь потока
ограничена, а обработчик автоматически удаляется при завершении итератора:

```python
async for meters in client.stream_level_meters(queue_size=64):
    for meter in meters.channels or []:
        print(meter.id, meter.level_left_percentage)
```

Также доступны `stream_focused_app_changes()`,
`stream_input_device_changes()` и общий `stream_events()`.

Полученное через RPC и уведомления состояние сохраняется в свойствах
`application_info`, `input_devices`, `output_devices`, `main_output`,
`channels`, `mixes`, `level_meters` и `focused_app`. Частичные уведомления
устройства объединяются с уже известным состоянием, не стирая отсутствующие
в уведомлении поля.

Обработчики одного соединения выполняются последовательно. Долгий обработчик
задерживает следующие события, поэтому тяжёлую работу лучше передавать в
отдельную задачу или очередь. При переполнении внутренней очереди новое событие
отбрасывается с предупреждением в журнале.

## Метаданные плагина

Если адаптер используется как часть интеграции со Stream Deck, Wave Link можно
сообщить семейства подключённых устройств:

```python
await client.set_plugin_info(["SD", "SDPlus"])
```

Последнее успешно отправленное значение сохраняется в памяти и повторно
отправляется после автоматического реконнекта.

## Низкоуровневый RPC

Для метода API без готовой обёртки используйте `call()`:

```python
result = await client.call("getChannels", None)
```

С параметрами и индивидуальным таймаутом:

```python
result = await client.call(
    "someMethod",
    {"id": "target-id"},
    timeout=2.0,
)
```

`call()` не проверяет структуру результата конкретного метода и не защищает от
изменения состояния Wave Link. Используйте его только для известных RPC-
методов и проверяйте возвращённые данные самостоятельно.

## Обработка ошибок

```python
from wavelink_core import (
    WaveLinkDisconnectedError,
    WaveLinkProtocolError,
    WaveLinkRpcError,
    WaveLinkTimeoutError,
)


try:
    await client.set_channel_level(channel_id, 0.5)
except WaveLinkTimeoutError as exc:
    print("Wave Link не ответил:", exc)
except WaveLinkDisconnectedError as exc:
    print("Соединение потеряно:", exc)
except WaveLinkRpcError as exc:
    print("RPC error:", exc.code, exc.message, exc.data)
except WaveLinkProtocolError as exc:
    print("Неожиданная структура ответа:", exc)
```

| Исключение | Когда возникает |
| --- | --- |
| `WaveLinkRpcError` | Wave Link вернул JSON-RPC `error`. |
| `WaveLinkProtocolError` | Ответ не соответствует ожидаемой структуре API. |
| `WaveLinkDisconnectedError` | Вызов сделан без соединения или соединение потеряно. |
| `WaveLinkTimeoutError` | Истёк таймаут RPC или ожидания реконнекта. |
| `ConnectionError` | Не удалось подключиться ни к одному найденному порту. |

`WaveLinkRpcError` сохраняет поля `code`, `message`, `data`, `method` и
`request_id`. Свойство `is_invalid_params` равно `True` для кода `-32602`.

## Переподключение

Автоматический реконнект запускается после потери ранее установленного
соединения. Первичный `connect()` сам по себе не повторяется бесконечно: если
Wave Link не запущен, он завершится `ConnectionError`, а решение о новой
попытке остаётся за вызывающим приложением.

После успешного реконнекта клиент пытается восстановить:

- последнее значение `set_plugin_info()`;
- последнее состояние каждой категории подписок.

Восстановление выполняется в режиме best effort. Ошибка восстановления
записывается в журнал, но само соединение остаётся рабочим.

Запрос, который выполнялся в момент обрыва, завершается
`WaveLinkDisconnectedError` и автоматически не повторяется. Это предотвращает
двойное выполнение изменяющих операций.

Явный `close()` отключает реконнект.
