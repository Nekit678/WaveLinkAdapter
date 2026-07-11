# Web Mixer Example

Полноценный пример использования `WaveLinkClient` в web-приложении. Python-
шлюз держит одно соединение с Wave Link, предоставляет браузеру JSON-RPC через
WebSocket и раздаёт статический сенсорный интерфейс на том же порту.

Из корня репозитория:

```bash
python -m examples.web_mixer.server
```

Откройте `http://127.0.0.1:8765`.

Для доступа с планшета в доверенной локальной сети:

```bash
python -m examples.web_mixer.server --host 0.0.0.0
```

После этого откройте `http://IP-КОМПЬЮТЕРА:8765`.

## Структура

```text
examples/web_mixer/
├── server.py       # WebSocket/JSON-RPC шлюз и HTTP-раздача
├── test_server.py  # интеграционные тесты примера
└── web/
    ├── index.html
    ├── app.js
    ├── styles.css
    ├── manifest.webmanifest
    └── favicon.svg
```

Пример намеренно не требует Node.js и сборщика: клиент написан на нативных
HTML, CSS и JavaScript. Для отключения раздачи интерфейса используйте
`--no-web-ui`.
