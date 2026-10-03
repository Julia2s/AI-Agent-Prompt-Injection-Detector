
## Быстрый запуск

Нужен запущенный Docker Desktop с Linux containers и Docker Compose v2.
В терминале из корня проекта:

```powershell
docker compose up --build -d
docker compose logs -f data-pipeline
```

Первый запуск скачивает Docker-образы и зависимости. После вывода JSON-контракта
нажми Ctrl+C, чтобы выйти из просмотра логов. Контейнеры продолжат работать.
В `docker compose ps -a` у `data-pipeline` должен быть `Exited (0)`:
это одноразовая задача, а не постоянно работающий сервер.

Открой MLflow: http://localhost:5000 → `prompt-injection` →
`infrastructure-data-eda`. В run открой Inputs/Datasets и Artifacts.
Исходное приложение доступно на http://localhost:8000/docs.

```powershell
New-Item -ItemType Directory -Force outputs
docker compose cp data-pipeline:/work/outputs/. ./outputs/
```

Внутри Compose tracking URI — `http://mlflow:5000`; с компьютера —
`http://localhost:5000`. После повторной обработки копируй результаты снова.
Для повторного запуска:

```powershell
docker compose run --rm data-pipeline
docker compose cp data-pipeline:/work/outputs/. ./outputs/
```

Выходы сохраняются в общем томе `data_outputs`, поэтому доступны также через
исходный контейнер `data-pipeline`. Каждый запуск создаёт новый run; данные
и digest при той же версии зависимостей остаются одинаковыми.

## Файлы и ответственность

| Путь | Назначение |
| --- | --- |
| `docker-compose.yml` | Сервисы, зависимости, healthchecks, тома, сеть и лимиты |
| `infra/Dockerfile.mlflow` | Окружение сервера и EDA, пользователь без root |
| `infra/requirements.in`, `infra/requirements.lock` | Прямые и все зафиксированные ML-зависимости |
| `infra/start_server.py` | Запуск MLflow с PostgreSQL и файловым Artifact Store |
| `mlops/data.py` | Проверки исходных данных и стабильное разбиение |
| `mlops/pipeline.py` | EDA, артефакты, Dataset Tracking, контракт |
| `mlops/handoff.py` | Проверяемая передача датасетов следующему run |
| `data/raw` | Оригинал, описание автора и манифест SHA-256 |
| `data/processed` | Готовые CSV из проверочного запуска |
| `docs/eda` | Реальные результаты EDA из проверочного запуска |
| `infra/tests` | Проверки данных и интеграции с MLflow |
| `docs/HANDOFF.md` | Контракт для второго участника |
| `docs/DEFENSE.md` | Объяснение решения и демонстрация |
| `docs/COMMITS.md` | Порядок переноса изменений в GitHub Desktop |
| `docs/VERIFICATION.md` | Выполненные проверки и ограничения |

## Данные и анализ

В наборе 662 запроса: официальный train содержит 546, test — 116.
Train разделён на 436 обучающих и 110 валидационных примеров с seed=42
и стратификацией. Пустых значений, нормализованных повторов и пересечений
между train/test в этой версии не найдено.

В обучении 162 атаки из 436 (37,2%), в test — 60 из 116 (51,7%). Доли отличаются,
поэтому accuracy нельзя сравнивать без учёта состава выборки. Основная будущая
метрика — F1 для атак, дополнительные — precision, recall и PR-AUC.
Подробности: [EDA](docs/eda/EDA.md), графики рядом.

Источник: https://huggingface.co/datasets/deepset/prompt-injections
Версия: `4f61ecb038e9c3fb77e21034b22511b523772cdd`.
Сохранены оригинальные данные и карточка автора, включая противоречивые указания
лицензии. Исходные parquet и README не изменяются; преобразования пишутся отдельно.

## Проверки без Docker
```powershell
uv venv .venv-ml --python 3.11
uv pip install --python .venv-ml/Scripts/python.exe -r infra/requirements.lock pytest==8.3.5 ruff==0.11.2
.venv-ml/Scripts/python.exe -m pytest infra/tests -q
.venv-ml/Scripts/python.exe -m ruff check mlops infra
.venv-ml/Scripts/python.exe -m ruff format --check mlops infra
```



