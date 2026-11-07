"""Train prompt-injection classifiers and register the best one in MLflow."""

import contextlib
import json
import logging
import os
from pathlib import Path

import matplotlib
import pandas as pd
from sklearn.pipeline import Pipeline

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

from mlops.run_logging import quiet_run
from src.core.logging import setup_logging

setup_logging("train")
logger = logging.getLogger(__name__)

TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT_NAME = "prompt-injection-experiments"
MODEL_NAME = "prompt-injection-detector"
HANDOFF_PATH = Path("outputs/handoff.json")
CHAMPION_ALIAS = "champion"
ARTIFACTS_DIR = Path("outputs/artifacts")

mlflow.set_tracking_uri(TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

contract = json.loads(HANDOFF_PATH.read_text(encoding="utf-8"))
if contract.get("experiment") and contract["experiment"] != EXPERIMENT_NAME:
    logger.warning(
        "Handoff experiment=%s differs from training experiment=%s",
        contract["experiment"], EXPERIMENT_NAME,
    )
data_run_id = contract["data_run_id"]
logger.info("Data run ID: %s", data_run_id)


def _load_split(uri: str) -> pd.DataFrame:
    """Download a CSV artifact from MLflow and return as DataFrame."""
    local_path = mlflow.artifacts.download_artifacts(artifact_uri=uri)
    return pd.read_csv(local_path, keep_default_na=False)


def _load_data() -> tuple:
    datasets = contract["datasets"]
    train_df = _load_split(datasets["train"]["artifact_uri"])
    val_df = _load_split(datasets["validation"]["artifact_uri"])
    return train_df, val_df


def _confusion_matrix_png(y_true, y_pred, title: str, out_dir: Path) -> Path:
    path = out_dir / f"confusion_matrix_{title}.png"
    fig, ax = plt.subplots(figsize=(6, 4))
    cm = confusion_matrix(y_true, y_pred)
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        ax=ax,
        xticklabels=["Benign", "Injection"],
        yticklabels=["Benign", "Injection"],
    )
    ax.set_title(f"Confusion Matrix - {title}")
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def run_experiment(run_name: str, vectorizer_params: dict, model) -> dict:
    run_dir = ARTIFACTS_DIR / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    with quiet_run(mlflow.start_run(run_name=run_name)) as run:
        run_id = run.info.run_id
        mlflow.set_tag("data_run_id", data_run_id)

        flat_params = {
            "model_type": model.__class__.__name__,
            **{f"vect__{k}": str(v) for k, v in vectorizer_params.items()},
        }
        for attr in ("C", "max_iter", "n_estimators", "max_depth", "min_samples_leaf"):
            if hasattr(model, attr):
                value = getattr(model, attr)
                if value is not None:
                    flat_params[f"model__{attr}"] = value
        mlflow.log_params(flat_params)

        pipeline = Pipeline(
            [
                ("vectorizer", TfidfVectorizer(**vectorizer_params)),
                ("classifier", model),
            ]
        )

        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_val)

        metrics = {
            "accuracy": accuracy_score(y_val, y_pred),
            "precision": precision_score(y_val, y_pred, zero_division=0),
            "recall": recall_score(y_val, y_pred, zero_division=0),
            "f1_score": f1_score(y_val, y_pred, zero_division=0),
        }
        mlflow.log_metrics(metrics)

        report_path = run_dir / f"classification_report_{run_name}.txt"
        report_path.write_text(
            classification_report(y_val, y_pred, zero_division=0),
            encoding="utf-8",
        )
        mlflow.log_artifact(str(report_path))

        cm_path = _confusion_matrix_png(y_val, y_pred, run_name, run_dir)
        mlflow.log_artifact(str(cm_path))

        mlflow.sklearn.log_model(sk_model=pipeline, artifact_path="model")

        logger.info(
            "Training run=%s run_id=%s f1=%.4f accuracy=%.4f",
            run_name, run_id, metrics["f1_score"], metrics["accuracy"],
        )
        return {"run_id": run_id, "metrics": metrics, "name": run_name}


with quiet_run(mlflow.start_run(run_name="training-session")) as session_run:
    mlflow.set_tag("stage", "training")
    train_df, val_df = _load_data()
    assert len(train_df) == contract["datasets"]["train"]["rows"], (
        f"Train rows mismatch: {len(train_df)} vs {contract['datasets']['train']['rows']}"
    )
    assert len(val_df) == contract["datasets"]["validation"]["rows"], (
        f"Val rows mismatch: {len(val_df)} vs {contract['datasets']['validation']['rows']}"
    )
    X_train, y_train = train_df["text"], train_df["label"]
    X_val, y_val = val_df["text"], val_df["label"]
    logger.info("Training rows=%s validation rows=%s", len(train_df), len(val_df))
    session_run_id = session_run.info.run_id

results = {}

results["baseline"] = run_experiment(
    run_name="baseline_tfidf_lr",
    vectorizer_params={"max_features": 5000},
    model=LogisticRegression(random_state=42, max_iter=1000),
)

results["improved"] = run_experiment(
    run_name="improved_tfidf_lr_bigrams",
    vectorizer_params={
        "max_features": 10000,
        "ngram_range": (1, 2),
        "stop_words": "english",
    },
    model=LogisticRegression(random_state=42, max_iter=1000),
)

results["rf"] = run_experiment(
    run_name="random_forest_tfidf",
    vectorizer_params={"max_features": 5000},
    model=RandomForestClassifier(n_estimators=100, random_state=42),
)

best_name, best_data = max(
    results.items(), key=lambda item: item[1]["metrics"]["f1_score"]
)
best_run_id = best_data["run_id"]
best_f1 = best_data["metrics"]["f1_score"]

logger.info("Best experiment=%s f1=%.4f run_id=%s", best_name, best_f1, best_run_id)

model_uri = f"runs:/{best_run_id}/model"
logger.info("Registering model from run_id=%s", best_run_id)

client = mlflow.tracking.MlflowClient()
with contextlib.suppress(mlflow.exceptions.RestException):
    client.create_registered_model(MODEL_NAME)

registered = client.create_model_version(
    name=MODEL_NAME,
    source=model_uri,
    run_id=best_run_id,
    description=f"Best run ({best_name}) with F1={best_f1:.4f}",
)
version = registered.version
logger.info("Model registered version=%s", version)

with contextlib.suppress(mlflow.exceptions.RestException):
    client.delete_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS)
client.set_registered_model_alias(
    name=MODEL_NAME, alias=CHAMPION_ALIAS, version=version
)
logger.info("Model alias=%s assigned to version=%s", CHAMPION_ALIAS, version)

summary = {
    "experiment": EXPERIMENT_NAME,
    "model_name": MODEL_NAME,
    "champion_alias": CHAMPION_ALIAS,
    "champion_version": version,
    "champion_run_id": best_run_id,
    "champion_run_name": best_name,
    "metrics": best_data["metrics"],
    "all_runs": {
        name: {"run_id": data["run_id"], "metrics": data["metrics"]}
        for name, data in results.items()
    },
}
summary_path = Path("outputs/training_summary.json")
summary_path.parent.mkdir(parents=True, exist_ok=True)
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
logger.info("Training completed; summary written to %s", summary_path)
