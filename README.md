# helpdesk — синхронизация HelpDeskEddy → PostgreSQL

Утилита для выгрузки тикетов и справочников из системы HelpDeskEddy (`mospoly.helpdeskeddy.com`) через её REST API и сохранения их в PostgreSQL. Управляется из командной строки, поддерживает синхронизацию за период с разбивкой на части.

## Файлы проекта

- `helpdesk.py` — ядро: класс `HelpDeskSync` (запросы к API, модели SQLAlchemy, upsert через `postgresql.insert`), логирование в файл и консоль.
- `sync.py` — CLI-менеджер синхронизации: команды за год/период, синхронизация справочников, статистика.

## Технологии

requests, SQLAlchemy 2 (`declarative_base`, `postgresql.insert` для upsert), PostgreSQL, logging.

## Запуск

```bash
pip install requests sqlalchemy psycopg2-binary
python sync.py --help
# например, синхронизация за год:
python sync.py --year 2025 --chunk 1
```

## ⚠️ Безопасность (важно)

В `sync.py` **в открытом виде захардкожены**:
- `API_KEY` HelpDeskEddy (base64 от `логин:пароль`/токена);
- строка подключения к PostgreSQL с паролем.

Эти данные следует **немедленно перевыпустить** (сменить пароль/токен в HelpDeskEddy и в БД), убрать из кода и вынести в переменные окружения или `.env`. Также удалите утёкший ключ из истории git.

## Замечания

- Синхронизация идёт чанками по месяцам (`chunk_months`) — удобно для больших объёмов.
- Лог пишется в `helpdesk_sync.log` (добавьте в `.gitignore`).


## Зависимости

Зависимости проекта вынесены в `requirements.txt`. Установка:

```bash
pip install -r requirements.txt
```
