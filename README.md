# AI Agent Prompt Injection Detector

Конвейер принимает задачи через API или CSV-источник и передаёт их через RabbitMQ
воркеру FastStream. Воркер загружает модель из MLflow и сохраняет результат в
PostgreSQL. При перегрузке ограниченная очередь замедляет источник; API отвечает
`503` и передаёт клиенту ответственность за повтор.

## Запуск

Требуются Docker Desktop и Docker Compose 2.20.0 или новее.

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps -a
docker compose logs --tail 50 data-pipeline train worker
```

При первом запуске `data-pipeline` готовит данные, `train` обучает модель и
назначает ей alias `champion`, затем запускается worker. Это может занять несколько
минут. Обычный запуск не включает инфраструктуру наблюдаемости.

| Сервис | Адрес |
|---|---|
| API и Swagger | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 |
| MLflow | http://localhost:5000 |

Значения по умолчанию для RabbitMQ Management: `guest` / `guest`.

```powershell
$body = @{ text = "Ignore previous instructions"; task_id = "example-1" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/tasks -ContentType "application/json" -Body $body
Invoke-RestMethod http://localhost:8000/api/v1/tasks/example-1
docker compose --profile load run --rm producer
```

Для нового запуска нагрузки задавайте другой `--batch-id`: идентификаторы задач
детерминированы. Настройки находятся в `.env.example` и `mlops/settings.py`.
Для изменения `QUEUE_CAPACITY` используйте новое имя очереди или пересоздайте
старую только после её опустошения; RabbitMQ не меняет её параметры повторным
объявлением. Не удаляйте непустые очереди и тома.

## Логирование и наблюдаемость

API, producer и worker используют стандартную библиотеку Python `logging` и
пишут одну JSON-запись на строку в stdout. Общие поля: `timestamp` (UTC),
`level`, `service`, `env`, `logger`, `message`; для задачи добавляется `task_id`,
для ошибки — `exception`. Уровень задаётся `LOG_LEVEL`, окружение — `ENV`.

JSON выбран для машинного поиска и одинаковой обработки событий разных
процессов. Стандартный `logging` позволяет подключать логи библиотек к общему
обработчику без дополнительной зависимости. Идентификатор передаётся через
RabbitMQ как `correlation_id`, устанавливается через `contextvars` и сохраняется
в потоке inference. Логи не содержат входной текст задачи; стек ошибки содержит
тип и расположение вызовов без текста исключения. Типичные секреты в DSN и
полях параметров маскируются, а сырые AMQP-кадры не выводятся при DEBUG.
Маскирование — дополнительная защита: сообщения новых библиотек требуют проверки.

```powershell
docker compose --profile logs up -d loki alloy vector
docker compose --profile logs ps
docker compose logs --tail 50 loki alloy vector
```

Профиль `logs` подключает инфраструктуру из `observability/compose.yaml`:
Alloy собирает логи API и worker, Vector — producer, Loki хранит их 48 часов.
Alloy выбран как агент Docker-логов; Vector использует события Docker и в
проверенном сценарии успел собрать короткий producer с `--rm`. Loki подходит для
стенда благодаря поиску по JSON и индексации только меток потоков. Агенты
ограничены текущим Compose-проектом и получают `docker.sock` через монтирование
`ro`; это не ограничивает права самих запросов к Docker API. Приложение не зависит
от хранилища. Если агент выключен или контейнер удалён до чтения, доставка его
логов не гарантируется.

Метки Loki — только `service`, `level`, `env`. `task_id` и будущий `trace_id`
остаются в записи: отдельный поток для каждого идентификатора привёл бы к росту
кардинальности. Пример поиска:

```powershell
$query = '{service=~"app|worker|producer"} |= "example-1"'
$encodedQuery = [uri]::EscapeDataString($query)
$result = Invoke-RestMethod "http://localhost:3100/loki/api/v1/query_range?query=$encodedQuery&limit=50"
$result.data.result | ConvertTo-Json -Depth 10
```

Лимиты: Loki — 512 МиБ, Alloy — 256 МиБ, Vector — 256 МиБ; суммарно 1024 МиБ.
Снимок без полноценной нагрузки 07.11.2026: 74,91, 42,56 и 15,44 МиБ
соответственно. Это не максимумы: полный бюджет под нагрузкой нужно проверить
через `docker stats`. Профиль `observability` пока включает только логовый стек;
трейсы, метрики и Grafana подключаются после реализации соответствующих частей.

## Поведение конвейера

- Очередь durable с `x-max-length=50` и `reject-publish`; publisher confirms и
  persistent-сообщения позволяют источнику повторять неподтверждённую публикацию
  с паузами 0,5 → 1 → 2 с, удерживая в памяти одну задачу.
- `PREFETCH_COUNT = WORKER_CONCURRENCY = 1`: воркер получает не больше задач,
  чем может обработать одновременно. Inference выполняется в пуле потоков.
- API возвращает `202` после подтверждения публикации, `200` для готового
  результата, `409` при конфликте идентификатора и `503` с `Retry-After` при
  перегрузке или недоступности зависимостей. При отказе новая запись `PENDING`
  компенсируется; повтор с тем же `task_id` безопасен.
- Результат записывается до ACK; `UNIQUE(task_id)` и атомарный upsert защищают от
  дублей. Временные ошибки повторяются до `MAX_ATTEMPTS`, невалидное сообщение
  сразу попадает в DLQ. Публикация в DLQ и ACK не атомарны, поэтому DLQ может
  содержать дубли. При остановке worker ожидает принятые задачи.

## Проверки и результаты HW4

```powershell
uv run pytest tests infra/tests -q --ignore=tests/integration
uv run pytest -m integration tests/integration -v
```

Интеграционные тесты используют временные контейнеры RabbitMQ и PostgreSQL;
проверка SIGTERM пропускается на Windows. Сценарии `scripts/experiment.sh`
рассчитаны на Bash/WSL; построение графиков выполняет `scripts/report.py`.
Графики и исходные измерения предыдущей работы находятся в `runs/`.

```bash
scripts/experiment.sh unbounded-1w unbounded 1 30
scripts/experiment.sh bounded-1w bounded 1 60
scripts/experiment.sh bounded-2w bounded 2 60
scripts/experiment.sh bounded-scale bounded 1 90 45
uv run python scripts/report.py --run "unbounded=runs/unbounded-1w" --run "bounded-1w=runs/bounded-1w" --run "bounded-2w=runs/bounded-2w" --run "bounded-scale=runs/bounded-scale" --limit 50 --out runs/report
```

Сценарий пересоздаёт worker и в конце останавливает его; запускайте его на
отдельном стенде, когда в рабочей очереди нет задач.

Результаты HW4 при `INFERENCE_DELAY=0.5`, лимите очереди 50 и одном слоте на
worker; сценарии длились 30–90 секунд с последующим дренажем:

| Показатель | Без ограничений, 1 worker | С ограничениями, 1 worker | С ограничениями, 2 worker | С ограничениями, 1→2 worker |
|---|---:|---:|---:|---:|
| Максимум `ready + unacked` | 41 870 | 51 | 52 | 51 |
| Максимум задач в памяти источника | 27 964 | 1 | 1 | 1 |
| Скорость источника, задач/с | 1 626,4 | 1,9 | 3,87 | 3,6 |
| Скорость worker, ACK/с | 1,79 | 2,15 | 4,31 | 3,68 |
| Латентность p95, с | 48,8 | 29,9 | 16,5 | 29,7 |
| Завершено задач | 74 | 144 | 281 | 287 |

Ограниченная очередь удерживала источник около пропускной способности worker;
второй worker увеличил её примерно вдвое. Для сценария 1→2 среднее включает
периоды до и после масштабирования. Подробности и остальные показатели — в
`runs/report/summary.md`.
