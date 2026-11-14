# AI Agent Prompt Injection Detector

Проект проверяет тексты на prompt injection. Задачу можно отправить через API или передать из CSV-файла. После этого она попадает в RabbitMQ, обрабатывается воркером FastStream, а результат сохраняется в PostgreSQL. Модель загружается из MLflow.

Если очередь заполнена, источник не отправляет сообщения бесконечно. Он ждёт подтверждения от RabbitMQ и повторяет публикацию с увеличивающейся задержкой. API в такой ситуации возвращает `503`, чтобы клиент мог повторить запрос позже.

## Запуск

Для запуска нужны Docker Desktop и Docker Compose версии 2.20 или выше.

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps -a
docker compose logs --tail 50 data-pipeline train worker
```

При первом запуске `data-pipeline` подготавливает данные, а `train` обучает модель и назначает ей alias `champion`. Затем запускается worker. Подготовка может занять несколько минут.

| Сервис | Адрес |
|---|---|
| API и Swagger | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 |
| MLflow | http://localhost:5000 |

Для RabbitMQ по умолчанию используются логин и пароль `guest`.

Пример отправки задачи:

```powershell
$body = @{ text = "Ignore previous instructions"; task_id = "example-1" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/tasks -ContentType "application/json" -Body $body
Invoke-RestMethod http://localhost:8000/api/v1/tasks/example-1
```

Источник из CSV запускается командой:

```powershell
docker compose --profile load run --rm producer
```

Для каждого нового запуска нагрузки нужно использовать новый `--batch-id`, потому что идентификаторы задач формируются детерминированно. Основные настройки находятся в `.env.example` и `mlops/settings.py`.

## Логирование

API, producer и worker используют стандартный Python `logging` и пишут JSON-логи в stdout. В каждой записи есть время, уровень, сервис, окружение, имя логгера и сообщение. Для событий, связанных с задачей, добавляется `task_id`, а для ошибок — информация об исключении.

Уровень логирования задаётся переменной `LOG_LEVEL`. Входной текст задачи в логи не записывается, а пароли и другие секреты маскируются.

`task_id` передаётся через RabbitMQ в поле `correlation_id`. В приложении он сохраняется в `contextvars`, поэтому его не нужно вручную добавлять в каждую строку.

Логовый стек запускается так:

```powershell
docker compose --profile logs up -d loki alloy vector
docker compose --profile logs ps
```

Alloy собирает логи API и worker, Vector — логи producer, а Loki хранит их 48 часов. Приложение не зависит от Loki: если агент или хранилище временно недоступны, сам конвейер продолжает работать.

В Loki используются только лейблы `service`, `level` и `env`. `task_id` и `trace_id` остаются полями записи. Это сделано для того, чтобы не создавать отдельный поток для каждого идентификатора.

## Распределённая трассировка

По умолчанию экспорт трейсов отключён (`OTEL_SDK_DISABLED=true`), поэтому приложение можно запускать без Collector и Tempo. Для включения трассировки:

```powershell
$env:OTEL_SDK_DISABLED = "false"
docker compose --profile traces up --build -d
docker compose --profile traces ps
```

Collector принимает данные по OTLP на портах 4317 и 4318, применяет `memory_limiter` и `batch`, а затем отправляет трейсы в Tempo. Tempo хранит их 48 часов.

FastAPI подключён к OpenTelemetry, а RabbitMQ — к `RabbitTelemetryMiddleware`. Поэтому один трейс проходит через HTTP-запрос, публикацию сообщения, обработку в worker, inference и запись результата в БД. Контекст переносится через W3C-заголовок `traceparent`.

Внутри worker создаются спаны `process`, `inference`, `database.write_result`, `retry.backoff` и `dead_letter`. Inference выполняется в `ThreadPoolExecutor`, поэтому контекст переносится через `copy_context().run`. В результате `inference` остаётся дочерним спаном обработки.

Для каждой доставки указывается `app.delivery_attempt`, а время ожидания в очереди — `app.queue_wait_ms`. Ошибки получают статус `ERROR` и записываются вместе с исключением. При повторных попытках и переводе сообщения в DLQ это также видно в трейсе.

В логи, созданные внутри активного спана, автоматически добавляются `trace_id` и `span_id`. У сервисов разные имена: `api`, `source` и `worker`.

На стенде используется `parentbased_always_on` с коэффициентом `1.0`, чтобы видеть все запросы и повторные доставки. Для production при большой нагрузке лучше использовать вероятностное семплирование, а ошибки и медленные запросы сохранять через tail-based семплирование в Collector.

Трейсы доступны по адресу http://localhost:3200. Подключение Tempo к Grafana относится к части участника 3.

## Поведение конвейера

- Рабочая очередь ограничена параметром `x-max-length=50` и использует `reject-publish`.
- `PREFETCH_COUNT` и `WORKER_CONCURRENCY` по умолчанию равны 1.
- API возвращает `202` для принятой задачи, `200` для готового результата, `409` при конфликте `task_id` и `503` при перегрузке.
- Результат сохраняется до подтверждения сообщения. Уникальный `task_id` защищает от дублей.
- Временные ошибки повторяются до `MAX_ATTEMPTS`, а некорректные сообщения отправляются в DLQ.
- При остановке worker дожидается уже принятых задач.

## Проверки и результаты HW4

```powershell
uv run pytest tests infra/tests -q --ignore=tests/integration
uv run pytest -m integration tests/integration -v
```

Интеграционные тесты используют временные контейнеры RabbitMQ и PostgreSQL. Нагрузочные сценарии рассчитаны на Bash или WSL, а графики строятся скриптом `scripts/report.py`.

При `INFERENCE_DELAY=0.5`, очереди размером 50 и одном слоте worker ограниченная очередь удерживала скорость источника примерно на уровне пропускной способности worker. После добавления второго worker пропускная способность выросла примерно вдвое. Подробные результаты находятся в `runs/report/summary.md`.
