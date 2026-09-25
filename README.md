# AI Agent Prompt Injection Detector

Асинхронное веб-приложение на FastAPI для детектирования prompt injection атак в AI-агентах.

## Архитектура проекта

```
src/
├── api/v1/routes/    # Эндпоинты API (healthz, version, health)
├── core/             # Конфигурация, БД, логирование
└── main.py           # Точка входа FastAPI
tests/                # Тесты с покрытием ≥80%
.github/workflows/    # CI/CD пайплайны
```

## Требования

- Python 3.11+
- Docker & Docker Compose
- Менеджер пакетов: `uv`

## Локальный запуск

### Без Docker

```bash
# Установка зависимостей
uv sync --dev

# Запуск приложения
uv run uvicorn src.main:app --reload
```

### С Docker Compose

```bash
# Сборка и запуск (приложение + PostgreSQL)
docker compose up --build

# Приложение доступно на http://localhost:8000
# PostgreSQL на порту 5432
```

## API Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/healthz` | Быстрая проверка живости (liveness probe) |
| GET | `/api/v1/version` | Версия приложения и имя |
| GET | `/api/v1/health` | End-to-end health check с латентностью БД и версиями зависимостей |

### Примеры запросов

```bash
# Liveness probe
curl http://localhost:8000/healthz

# Версия приложения
curl http://localhost:8000/api/v1/version

# Полная проверка здоровья
curl http://localhost:8000/api/v1/health
```

## Тестирование

```bash
# Запуск тестов с измерением покрытия
uv run pytest --cov=src tests/ --cov-report=term-missing --cov-fail-under=80
```

Минимальный порог покрытия: **80%**.

## Pre-commit хуки

Проект использует pre-commit для автоматических проверок перед коммитом:

- `trailing-whitespace` — удаление лишних пробелов
- `end-of-file-fixer` — пустая строка в конце файла
- `check-yaml` — валидация YAML
- `check-added-large-files` — запрет больших файлов (>1MB)
- `ruff` — линтер Python
- `ruff-format` — форматирование кода

Установка хуков:
```bash
uv run pre-commit install
```

## CI/CD Пайплайны

### CI (Continuous Integration)

Запускается автоматически при:
- Push в ветки `main` и `develop`
- Pull Request в `main`

**Что проверяет:**
- Линтинг (Ruff)
- Форматирование кода
- Запуск тестов

Файл: `.github/workflows/ci.yml`

### CD (Continuous Delivery)

Запускается автоматически при:
- Push тега формата `v*.*.*` (например, `v0.1.0`)
- Push в ветку `main`

**Что делает:**
- Собирает Docker-образ
- Публикует образ в GitHub Container Registry (GHCR)

**Стратегия тегирования образов:**
- `v{version}` — для релизных тегов (например, `v0.1.0`)
- `main` — для актуальной версии из ветки main
- `{short_sha}` — хеш коммита для точной идентификации

Файл: `.github/workflows/cd.yml`

### Обоснование условий запуска CD

Пайплайн сборки и публикации образа настроен на запуск в двух случаях:

1. **При создании Git-тега формата `v*.*.*`** — используется для фиксации стабильных релизных версий приложения. Образ получает семантический тег (например, `v0.1.0`), что гарантирует воспроизводимость и возможность отката к конкретной версии.

2. **При push в ветку `main`** — используется для автоматического обновления образа с тегом `main`. Это обеспечивает актуальность артефакта для staging-окружения сразу после успешного прохождения CI и code review.

Такая стратегия разделяет потоки ежедневной разработки и официальных релизов, соответствуя лучшим практикам GitOps и позволяя командам выбирать нужную версию образа для развёртывания.

## Структура Docker

### Dockerfile

- Multi-stage сборка (builder + runner)
- Запуск от non-root пользователя (`appuser`)
- Минимальный образ на базе `python:3.11-slim`

### docker-compose.yml

- Сервис `app` — приложение FastAPI
- Сервис `db` — PostgreSQL 15 Alpine
- Healthcheck для БД
- Лимиты логирования (max-size: 10m, max-file: 3)
- Volume для персистентности данных БД

## Логирование

Приложение использует `loguru` для структурированного логирования:

- Консольный вывод с цветным форматированием
- Файловое логирование в `logs/app.log`
- Ротация логов: 10 MB
- Хранение: 7 дней
- Сжатие: gzip

## Зависимости

### Runtime
- `fastapi` — веб-фреймворк
- `uvicorn` — ASGI сервер
- `asyncpg` — асинхронный драйвер PostgreSQL
- `sqlalchemy[asyncio]` — ORM
- `pydantic-settings` — управление конфигурацией
- `loguru` — логирование

### Development
- `ruff` — линтер и форматтер
- `pytest` — тестирование
- `pytest-asyncio` — асинхронные тесты
- `httpx` — HTTP клиент для тестов
- `pytest-cov` — измерение покрытия
- `pre-commit` — хуки git
