# Prompt Injection Detector

Detection service for prompt injection attacks in AI agent inputs.
MLflow tracks data lineage, experiments and the registered model.

## Architecture

```
Dataset (deepset/prompt-injections)
       ↓
data-pipeline  ─→  MLflow Tracking Server  ←── MLflow Model Registry
  EDA / splits        PostgreSQL backend            prompt-injection-detector
       ↓                                              ↑
  handoff.json ─→  train.py (experiments) ─→  /process endpoint
```

- **`app`** — FastAPI service, `POST /process`, inference via `models:/prompt-injection-detector@champion`.
- **`db`** — application PostgreSQL.
- **`mlflow` / `mlflow-db`** — MLflow Tracking Server with its own PostgreSQL backend and file-based Artifact Store.
- **`data-pipeline`** — one-shot job: validates raw data, splits, runs EDA, logs everything to MLflow, writes `handoff.json`.

## Quick start

```bash
docker compose up --build -d
docker compose logs -f data-pipeline          # wait until the pipeline run finishes
```

| Service | URL |
| --- | --- |
| MLflow UI | http://localhost:5000 |
| API docs | http://localhost:8000/docs |
| API health | http://localhost:8000/healthz |

Run the pipeline again:

```bash
docker compose run --rm data-pipeline
docker compose cp data-pipeline:/work/outputs/. ./outputs/
```

## MLflow

Open http://localhost:5000 → experiment `prompt-injection` → run `infrastructure-data-eda`.

- **Inputs / Datasets** — lineage: raw data, train / validation / test splits, all tied to the same Hugging Face revision.
- **Artifacts** — EDA CSV/JSON, class-balance and length plots, raw data, processed CSVs, `handoff.json`.
- **Experiment `prompt-injection-experiments`** — training runs: `baseline_tfidf_lr`, `improved_tfidf_lr_bigrams`, `random_forest_tfidf`. Each run logs accuracy, precision, recall, F1, a classification report and a confusion matrix.
- **Model Registry** — best run is registered as `prompt-injection-detector` and aliased `champion`.

Run training locally (requires MLflow server at `http://localhost:5000` and `outputs/handoff.json`):

```bash
python -m mlops.train
```

## Dataset

| | Rows | Injections | Benign |
| --- | --- | --- | --- |
| Train | 436 | 162 (37.2 %) | 274 |
| Validation | 110 | 41 (37.3 %) | 69 |
| Test | 116 | 60 (51.7 %) | 56 |

Source: [deepset/prompt-injections](https://huggingface.co/datasets/deepset/prompt-injections), revision `4f61ecb038e9c3fb77e21034b22511b523772cdd`.
Primary metric: **F1 for class 1 (injection)**. Accuracy is misleading because the test split has a very different class balance.

## Project structure

```
├── docker-compose.yml        # services, healthchecks, volumes
├── Dockerfile                 # FastAPI application image
├── infra/
│   ├── Dockerfile.mlflow      # MLflow server + pipeline image
│   ├── requirements.lock      # pinned ML dependencies
│   ├── start_server.py        # MLflow startup script
│   ── tests/                 # data-quality and integration tests
├── mlops/
│   ├── data.py                # data loading and validation
│   ├── pipeline.py            # EDA, splits, MLflow logging, handoff
│   ├── handoff.py             # contract helper for the next run
│   └── train.py               # training experiments and model registry
├── data/
│   ├── raw/                   # original dataset + SHA-256 manifest
│   └── processed/             # CSV splits from the pipeline
├── docs/
│   └── eda/                   # EDA report and plots
├── src/                       # FastAPI application
│   └── api/v1/
└── tests/                     # API tests
```

## Verification (no Docker)

```bash
uv venv .venv-ml --python 3.11
uv pip install --python .venv-ml/Scripts/python.exe -r infra/requirements.lock pytest==8.3.5 ruff==0.11.2
.venv-ml/Scripts/python.exe -m pytest infra/tests -q
.venv-ml/Scripts/python.exe -m ruff check mlops infra
.venv-ml/Scripts/python.exe -m ruff format --check mlops infra
```
