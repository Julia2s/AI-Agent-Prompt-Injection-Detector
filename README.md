# AI Agent Prompt Injection Detector

Учебный MLOps-конвейер для проверки текстов на prompt injection:

**FastAPI / CSV-источник → ограниченная очередь RabbitMQ → FastStream-worker → PostgreSQL.**

Worker загружает модель из MLflow по alias `champion` и сохраняет результат до подтверждения сообщения. Наблюдаемость реализована через OpenTelemetry, Loki, Tempo, VictoriaMetrics и Grafana.

## Требования

- Docker Engine или Docker Desktop с Linux-контейнерами.
- Docker Compose 2.20 или новее: используется `include`.
- Python 3.11 и `uv` для локальных тестов.
- PowerShell для команд ниже; Bash/WSL для `scripts/experiment.sh`.

Все команды выполняются из корня проекта.

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Переменные текущего PowerShell имеют приоритет над `.env` при подстановке Compose. После изменения окружения контейнеры приложения нужно пересоздать через `up`: обычный `restart` не обновляет их окружение.

## Запуск приложения без наблюдаемости

```powershell
$env:OTEL_SDK_DISABLED = "true"

docker compose up -d --build --wait
docker compose ps -a
```

При первом запуске `data-pipeline` подготавливает данные, а `train` обучает модель, регистрирует её в MLflow и назначает alias `champion`. После этого запускается worker.

Успешно завершившиеся одноразовые контейнеры подготовки и обучения имеют статус `Exited (0)`.

Обычный запуск не создаёт сервисы наблюдаемости. Уже запущенные контейнеры других профилей автоматически не останавливаются.

| Сервис | Адрес |
|---|---|
| API / Swagger | http://localhost:8000/docs |
| Health | http://localhost:8000/healthz |
| RabbitMQ Management | http://localhost:15672 |
| MLflow | http://localhost:5000 |
| Grafana, при включённом профиле | http://localhost:3000 |

RabbitMQ использует стендовые `guest` / `guest`. Пользователь Grafana — `admin`, пароль задаётся через `GRAFANA_ADMIN_PASSWORD`.

## Полный стек наблюдаемости

```powershell
$env:OTEL_SDK_DISABLED = "false"
$env:OTEL_TRACES_EXPORTER = "otlp"
$env:OTEL_METRICS_EXPORTER = "otlp"

docker compose --profile observability up -d --build --wait
docker compose --profile load build producer
```

Producer находится в профиле `load`, поэтому собирается отдельно. После изменения его кода необходимо повторить `build producer`.

В Grafana автоматически появляются:

- datasource VictoriaMetrics, Loki и Tempo;
- дашборд «Конвейер» с выбором worker;
- алерты заполнения рабочей очереди и наличия сообщений в DLQ;
- переходы из логов в трейс и обратно.

Конфигурация хранится в `observability/grafana`. До отправки задач пустые графики на новом стенде допустимы.

### Профили

| Профиль | Дополнительные сервисы | Экспорт трейсов | Экспорт метрик |
|---|---|---|---|
| `logs` | Loki, Alloy, Vector | `none` | `none` |
| `traces` | Collector трейсов, Tempo | `otlp` | `none` |
| `metrics` | Collector метрик, VictoriaMetrics | `none` | `otlp` |
| `observability` | Все сервисы наблюдаемости и Grafana | `otlp` | `otlp` |

Экспорт управляется `OTEL_TRACES_EXPORTER` и `OTEL_METRICS_EXPORTER`. Для экспорта также требуется `OTEL_SDK_DISABLED=false`.

Например, только трейсы:

```powershell
$env:OTEL_SDK_DISABLED = "false"
$env:OTEL_TRACES_EXPORTER = "otlp"
$env:OTEL_METRICS_EXPORTER = "none"

docker compose --profile traces up -d --build --wait
```

Проверка конфигураций без запуска:

```powershell
docker compose config --quiet
docker compose --profile logs config --quiet
docker compose --profile traces config --quiet
docker compose --profile metrics config --quiet
docker compose --profile observability config --quiet
```

### Разделение Collector

| Сигнал | Переменная | Адрес внутри Compose | Порты на хосте |
|---|---|---|---|
| Трейсы | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | gRPC 4317, HTTP 4318 |
| Метрики | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | `http://otel-metrics-collector:4317` | gRPC 4327, HTTP 4328 |

Collector трейсов зависит только от Tempo, Collector метрик — только от VictoriaMetrics. Оба используют `memory_limiter` и `batch`.

Внутри обоих контейнеров OTLP слушает порты 4317/4318. Отправка метрик в Collector только для трейсов вызывает `UNIMPLEMENTED / unknown MetricsService`.

Проверенная закреплённая версия Collector — `0.165.0`. Версии образов задаются в `.env.example` и Compose.

## Отправка задачи через API

```powershell
$taskId = "example-" + [guid]::NewGuid().ToString("N")
$body = @{
    task_id = $taskId
    text = "Summarize the attached report."
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8000/api/v1/tasks" -Method Post -ContentType "application/json" -Body $body

Start-Sleep -Seconds 2

Invoke-RestMethod -Uri "http://localhost:8000/api/v1/tasks/$taskId"
```

POST принятой задачи возвращает `202` и `PENDING`. После обработки GET возвращает `SUCCESS`, классификацию, время inference и полную задержку. При заполненной очереди ожидание результата будет дольше.

| Ситуация | Ответ |
|---|---|
| Новая задача принята брокером | `202` |
| Уже завершённая задача с тем же ID и текстом | `200` |
| Повтор ID с другим текстом | `409` |
| Невалидное тело HTTP-запроса | `422` |
| Очередь заполнена или публикация недоступна | `503` с `Retry-After` |
| Хранилище результатов недоступно | `503` с отдельной причиной |

API создаёт `PENDING`, затем публикует сообщение с publisher confirm. При отказе публикации новая запись компенсируется удалением.

Worker сохраняет результат до ACK. Доставка допускает повторы; уникальный `task_id` и проверка сохранённого результата обеспечивают идемпотентность результата. Это не гарантия единственного выполнения inference при любых сбоях.

## Нагрузка и второй worker

Для демонстрации Grafana используется очередь `inference_queue_v2` с лимитом 50 — именно её показывают дашборд и алерты.

```powershell
$env:QUEUE_NAME = "inference_queue_v2"
$env:QUEUE_CAPACITY = "50"
$env:WORKER_CONCURRENCY = "1"
$env:PREFETCH_COUNT = "1"
$env:INFERENCE_DELAY = "0.5"
$env:OTEL_SDK_DISABLED = "false"
$env:OTEL_TRACES_EXPORTER = "otlp"
$env:OTEL_METRICS_EXPORTER = "otlp"

docker compose --profile observability up -d --build --scale worker=1 --wait
docker compose --profile load build producer

$batchId = "demo-" + [guid]::NewGuid().ToString("N")

docker compose --profile load run --rm --no-deps producer python -m mlops.producer --batch-id $batchId --repeat 100000 --mode backpressure --duration 180
```

Каждому прогону нужен новый `--batch-id`, иначе задачи могут распознаваться как уже завершённые дубликаты.

При заполнении очереди RabbitMQ отклоняет публикацию через `reject-publish`. Источник учитывает отказ и повторяет отправку с backoff, удерживая одну текущую задачу в буфере.

Три минуты нагрузки оставляют время на заполнение очереди, сбор метрик и срабатывание алерта с `for: 1m`.

После первого прогона в том же PowerShell:

```powershell
docker compose up -d --no-deps --no-recreate --scale worker=2 --wait worker

$batchId = "demo-" + [guid]::NewGuid().ToString("N")

docker compose --profile load run --rm --no-deps producer python -m mlops.producer --batch-id $batchId --repeat 100000 --mode backpressure --duration 180
```

В контрольном прогоне два worker давали примерно по 1,9 задачи/с, суммарно около 3,8 задачи/с. Установившаяся скорость источника приближалась к общей пропускной способности worker.

`WORKER_INSTANCE_ID` для масштабируемых реплик оставляем пустым: идентификатор берётся из hostname контейнера.

Скрипт `scripts/experiment.sh` сохраняет прежние сравнительные эксперименты HW4 и создаёт очереди `exp_...`. Они не совпадают с фиксированными запросами дашборда HW5. Результаты предыдущей работы находятся в `runs/report/summary.md`.

## Логирование

Используется стандартный Python `logging`: собственные и библиотечные записи приведены к JSON в stdout. Уровень задаётся `LOG_LEVEL`.

Основные поля: `timestamp`, `level`, `service`, `env`, `logger`, `message`. Для событий задачи добавляется `task_id`, для ошибок — очищенный traceback. Внутри активного спана автоматически добавляются `trace_id` и `span_id`.

`task_id` передаётся как RabbitMQ `correlation_id` и устанавливается в `contextvars`. Пароли DSN и другие секреты маскируются; полный текст задачи в логи не включается.

Alloy собирает логи API и worker, Vector — producer. Агенты фильтруют контейнеры по Compose-проекту и получают Docker socket только на чтение. Приложение не отправляет логи непосредственно в Loki.

В Loki индексируются только низкокардинальные метки `service`, `level`, `env`. Метка контейнера API может называться `app`, тогда как JSON-поле `service` содержит `api`.

Поиск истории задачи в **Explore → Loki → Code**:

```logql
{service=~".+"} | json | task_id="TASK_ID"
```

Поиск отказов API:

```logql
{service=~".+"} |= "Returning HTTP 503"
```

Ошибки за последние 15 минут ищутся выбором соответствующего диапазона времени и запросом:

```logql
{service=~".+", level=~"ERROR|CRITICAL"}
```

`task_id` и `trace_id` остаются полями JSON. Их отображение после `| json` не означает добавления в постоянные индексные метки потока.

## Трейсинг и корреляция

OpenTelemetry SDK отправляет трейсы через Collector в Tempo. Просмотр выполняется в Grafana Explore.

Сквозной трейс содержит:

**API POST → публикация RabbitMQ → обработка worker → inference → запись в БД.**

Через брокер передаётся W3C `traceparent`. Контекст в `ThreadPoolExecutor` явно переносится через `copy_context().run`, поэтому inference остаётся дочерним спаном обработки.

В атрибутах присутствуют ID задачи, номер попытки, alias и фактическая версия модели, имя сервиса, версия, окружение и идентификатор экземпляра.

### Переходы

1. Найти задачу в Loki по `task_id`.
2. Раскрыть `Task accepted` или `Task result saved`, нажать **Open trace**.
3. В Tempo открыть `inference`, проверить alias, версию и длительность.
4. Открыть `process` и нажать **Logs for this span**.

Обратный переход ищет `trace_id` в Loki. Настроены отступы `spanStartTimeShift: "-1m"` и `spanEndTimeShift: "1m"`, чтобы короткий спан БД не исключал соседний лог о сохранении результата.

Некоторые библиотечные записи `Received` / `Processed` создаются вне активного спана и могут не содержать `trace_id`. Полная история ищется по `task_id`.

### Ошибки и семплирование

Ошибки получают статус `ERROR`, исключение и номер попытки. Повторы и перевод в DLQ видны в одном трейсе. Невалидный JSON направляется в DLQ сразу; ошибки inference повторяются до `MAX_ATTEMPTS`.

Ответ 503 записывает `exception`, `http.response.status_code=503` и событие `api.response.503`.

Поиск таких трейсов:

```traceql
{ resource.service.name = "api" && span.http.response.status_code = 503 }
```

На стенде используется `parentbased_always_on`; корневые трейсы сохраняются полностью, решение входящего parent-контекста уважается. Семплирование настраивается через `OTEL_TRACES_SAMPLER` и `OTEL_TRACES_SAMPLER_ARG`.

Для большого потока можно применять вероятностное head-based семплирование. Tail-based сохранение ошибок требует доставки соответствующих спанов в Collector: оно не восстановит трейсы, уже отброшенные SDK. Tail-based pipeline в текущем стенде не включён.

### Семантика времени ожидания

`app.queue_wait_ms` вычисляется от `created_at` до начала текущей попытки worker. Это возраст задачи до обработки: у CSV-источника он может включать ожидание публикации и backoff, а при повторной доставке — предыдущие попытки.

Это не отдельный точный замер исключительно нахождения в RabbitMQ. Для анализа используются таймлайн публикации и обработки с учётом этой семантики.

Полная задержка до сохранения результата хранится в `latency_ms`. SLO на полную задержку нельзя проверять только по длительности inference.

## Метрики

| Компонент | Показатели |
|---|---|
| API | Запросы по маршруту и коду ответа, гистограмма длительности |
| Источник | Опубликованные задачи, отказы брокера, суммарный backoff |
| Worker | Результаты `ok/error/dlq`, длительность inference, inflight, повторные доставки |
| Процессы | RSS |
| RabbitMQ | Глубина рабочей очереди и DLQ, unacked из Prometheus-плагина |

Учёт результатов worker:

- `ok` — после сохранения и проверки результата;
- `error` — при исключении, вышедшем из обработки;
- `dlq` — после успешной публикации в DLQ;
- последняя попытка, успешно ушедшая в DLQ, считается как `dlq`;
- завершённый дубликат не добавляет новый `ok`.

Inflight увеличивается и уменьшается сбалансированно, включая ошибки.

API использует шаблоны маршрутов, например `/api/v1/tasks/{task_id}`. Все неизвестные URL имеют одну метку `unmatched`. Необработанные исключения HTTP учитываются как 500.

### Сбор и бакеты

Приложения отправляют метрики по OTLP. Push подходит короткоживущему producer: периодический pull мог бы пропустить весь его жизненный цикл. При завершении выполняются `force_flush` и закрытие провайдера; это попытка доставки с ограничением времени, а не гарантия при недоступном Collector.

RabbitMQ — долгоживущий сервис. Его Prometheus endpoint на порту 15692 опрашивается Collector каждые 15 секунд.

Границы гистограмм, секунды:

- inference: `0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5`;
- API: `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5`.

`histogram_quantile` интерполирует внутри бакетов. Поэтому p50 около 625 мс не противоречит отдельному inference около 508 мс при попадании наблюдений в бакет 500–750 мс.

Перцентили разных worker не усредняются. Сначала агрегируются скорости бакетов с сохранением `le`:

```promql
histogram_quantile(0.95,
  sum by (le) (rate(worker_inference_duration_seconds_bucket[1m]))
)
```

### Кардинальность

Обозначения: `A` — экземпляры API, `W` — worker, `P` — источники, `R` — маршруты с учётом `unmatched`, `S` — коды ответа, `M` — alias модели, `E` — типы отказов брокера.

| Метрика | Оценка рядов |
|---|---:|
| API counter | `A × R × S` |
| API histogram | `A × R × 11` |
| Worker counter результатов | `W × 3` |
| Worker histogram | `W × M × 12` |
| Inflight и redeliveries | `W × 2` |
| Метрики источника | `P × (1 + E + 1)` |
| RSS | `A + W + P` |

В гистограммах учитываются конечные границы, `+Inf`, `sum` и `count`: у API 11 рядов на набор меток, у inference — 12.

Например, при `A=1`, `W=2`, `P=1`, `R=8`, `S=7`, `M=1`, `E=3` получается:

`56 + 88 + 6 + 24 + 4 + 5 + 4 = 187`

Это иллюстративная оценка прикладных метрик с указанными предположениями. Дополнительно экспортируются метрики RabbitMQ и служебные ряды.

Фактическое число активных рядов можно проверить в VictoriaMetrics:

```promql
count({__name__=~".+"})
```

Метка `task_id` с 6000 значениями может умножить затронутые ряды примерно на 6000. Поэтому ID задач и трейсов используются в логах и спанах.

## Алерты

Правила хранятся в `observability/grafana/provisioning/alerting/rules.yaml`. Интервал оценки — 30 секунд.

| Правило | Условие | `for` |
|---|---|---|
| Заполнение рабочей очереди | `inference_queue_v2`: ready > 45 | 1 минута |
| Сообщения в DLQ | `inference_queue_v2.dlq`: ready > 0 | 30 секунд |

Порог 45 соответствует 90% лимита 50. Выдержки исключают короткие переходные всплески.

Проверены переходы `Pending → Alerting/Firing → Normal` для заполнения очереди и `Pending → Alerting/Firing` для DLQ. Пока сообщения остаются в DLQ, её алерт продолжает срабатывать.

Порог, линия лимита и запросы согласованы с очередью размером 50. При смене имени или лимита нужно обновлять все соответствующие настройки.

`No data` сейчас настроено как `OK`; это не отдельный контроль доступности сбора. Внешняя доставка уведомлений не настраивалась.

## Память и хранение

Контрольный замер `docker stats --no-stream` от 20.11.2026 выполнен под нагрузкой при двух worker.

| Сервис наблюдаемости | Используется, МиБ | Лимит, МиБ |
|---|---:|---:|
| Loki | 64,61 | 384 |
| Alloy | 68,78 | 128 |
| Vector | 18,93 | 128 |
| Tempo | 220,90 | 256 |
| Collector трейсов | 39,92 | 128 |
| Collector метрик | 108,10 | 128 |
| VictoriaMetrics | 156,50 | 192 |
| Grafana | 56,67 | 256 |
| **Всего** | **734,41** | **1600** |

Фактический замер — около **0,72 ГиБ**, сумма лимитов — **1,56 ГиБ**. Один срез не является измерением пикового потребления.

API, worker, RabbitMQ, MLflow и PostgreSQL относятся к основному конвейеру и в эту сумму не входят.

Retention Loki и Tempo — 48 часов, VictoriaMetrics — 72 часа. Хранилища используют именованные тома; у хранилищ и Grafana есть healthchecks.

Loki выбран для структурированных логов с небольшим набором индексных меток, Tempo — для OTLP-трейсов и корреляции в Grafana, VictoriaMetrics — как PromQL-совместимое хранилище, уложившееся в измеренный бюджет стенда.

## Результаты ручных проверок

| Проверка | Результат |
|---|---|
| Нагрузка | Очередь достигает лимита 50, источник получает отказы и замедляется |
| Два worker | Различимы по экземплярам; суммарная скорость на контрольном участке около 3,8 задачи/с |
| История задачи | Найдены записи API и worker с общим task_id и trace_id |
| Сквозной трейс | API, publish, worker, inference и запись в БД; alias и фактическая версия присутствуют |
| Корреляция | Проверены оба перехода: логи → трейс → логи |
| HTTP-перегрузка | Получены 503, WARNING-логи, метрики и ERROR-трейсы с exception и api.response.503 |
| Ошибка inference | Временная инъекция ошибки дала попытки 1, 2, 3 и перевод в DLQ в одном трейсе |
| SIGTERM | Во время inference около 20 с worker получил сигнал, завершил задачу и сохранил результат; stop ожидал около 18,9 с |
| Collector недоступны | Задача завершилась SUCCESS при остановленных обоих Collector |
| Новые тома | Под отдельным Compose-проектом автоматически появились datasource, дашборд и правила; задача прошла PENDING → SUCCESS |

Инъекция ошибки выполнялась только во временном процессе worker для маркерной задачи; файлы приложения не менялись. После проверки обычные worker восстановлены.

Текущий API имеет состояния `PENDING` и `SUCCESS`. Отдельный статус `FAILED/DLQ` не реализован; терминальная ошибка подтверждается содержимым DLQ, логами и трейсом.

В тесте отключения Collector inference составил около 508/511 мс до/во время остановки. Время двух POST составило 2057/16 мс: по двум запросам нельзя делать вывод об ускорении или стабильных перцентилях. Подтверждено продолжение обработки.

При недоступности Collector SDK повторяет экспорт и использует ограниченные буферы. Длительный сбой может привести к потере части телеметрии. Логи собираются отдельными агентами, поэтому остановка Collector не отключает их отправку в Loki.

## Запуск с чистыми томами

Проверочный проект получает случайное имя. Исходный стенд временно останавливается для освобождения портов, его тома сохраняются. Перед переключением дождаться завершения producer.

```powershell
$originalProject = (docker compose config --format json | ConvertFrom-Json).name
$cleanProject = "cleancheck-" + [guid]::NewGuid().ToString("N").Substring(0, 8)

$env:OTEL_SDK_DISABLED = "false"
$env:OTEL_TRACES_EXPORTER = "otlp"
$env:OTEL_METRICS_EXPORTER = "otlp"

docker compose -p $originalProject --profile observability stop
docker compose -p $cleanProject --profile observability up -d --build --wait

docker compose -p $cleanProject --profile observability ps -a
Invoke-RestMethod -Uri "http://localhost:8000/healthz"
```

Проверить Grafana, три datasource, дашборд, два Provisioned-правила и одну задачу через API. Пустые графики до нагрузки допустимы.

На новых томах MLflow создаёт новую историю версий модели, поэтому номер не обязан совпадать с прежним стендом.

Возврат:

```powershell
docker compose -p $cleanProject --profile observability down
docker compose -p $originalProject --profile observability start
```

`down` сохраняет тома. Вариант `down -v` удаляет данные выбранного проекта, включая БД и артефакты MLflow. Для полного сброса следует использовать отдельный проверочный проект или заранее сохранённые данные.

## Автоматические проверки

```powershell
uv sync --frozen
$env:OTEL_SDK_DISABLED = "true"

uv run ruff check .
uv run ruff format --check .
uv run pytest -m "not integration" tests -q --cov=src --cov-report=term-missing --cov-fail-under=80
uv run pytest -m integration tests/integration -q --tb=short
```

Для следующего запуска демонстрационного стека вернуть:

```powershell
$env:OTEL_SDK_DISABLED = "false"
```

Контрольный набор после добавления `tests/test_api_metrics.py`:

- **66 passed, 4 deselected**;
- покрытие `src` — **80,92%**, порог — 80%;
- интеграционные тесты в Linux — **4 passed**;
- на Windows — **3 passed, 1 skipped**: нативный SIGTERM-тест пропускается по платформе, остановка дополнительно проверена в Compose.

Интеграционные тесты используют собственные временные контейнеры RabbitMQ и PostgreSQL. Новые HTTP-тесты проверяют учёт 200/503/500 и общую метку неизвестных URL.

Прохождение coverage не заменяет итог pytest: отдельно должны отсутствовать `failed` и `errors`.

## Основные файлы

- `src/main.py` — API и HTTP-метрики.
- `src/core/logging.py`, `src/core/tracing.py` — логи и трейсы.
- `mlops/producer.py`, `mlops/worker.py` — источник и обработчик.
- `mlops/messaging.py` — очереди, publisher confirms и middleware брокера.
- `mlops/errors.py` — общие исключения без запуска producer при импорте.
- `mlops/otel_metrics.py` — метрики и однократное завершение провайдеров.
- `mlops/data.py`, `mlops/train.py` — подготовка данных и обучение.
- `observability/compose.yaml` — стек наблюдаемости.
- `observability/otel-collector.yaml` — pipeline трейсов.
- `observability/otel-metrics-collector.yaml` — pipeline метрик.
- `observability/grafana/` — datasource, дашборд и алерты.
- `scripts/experiment.sh`, `scripts/monitor.py`, `scripts/report.py` — эксперименты и отчёт HW4.
- `runs/report/summary.md` — результаты предыдущей работы.