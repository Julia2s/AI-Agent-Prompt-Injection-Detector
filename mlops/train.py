"""Train prompt-injection classifiers and register the best one in MLflow."""

import json
import os
from pathlib import Path

import matplotlib
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

from mlops.handoff import load_and_log_inputs

TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = "prompt-injection-experiments"
MODEL_NAME = "prompt-injection-detector"
HANDOFF_PATH = Path(__file__).resolve().parents[1] / "outputs/handoff.json"
CHAMPION_ALIAS = "champion"
ARTIFACTS_DIR = Path("outputs/artifacts")


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


def run_experiment(
    run_name: str, vectorizer_params: dict, model, train_df, val_df, contract
) -> dict:
    run_dir = ARTIFACTS_DIR / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        mlflow.set_tags(
            {
                "data_run_id": contract["data_run_id"],
                "validation_sha256": contract["datasets"]["validation"]["sha256"],
                "evaluation_method": "f1-positive-class-1-v1",
            }
        )

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

        pipeline.fit(train_df["text"], train_df["label"])
        y_val = val_df["label"]
        y_pred = pipeline.predict(val_df["text"])

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

        print(f"  F1: {metrics['f1_score']:.4f}, Accuracy: {metrics['accuracy']:.4f}")
        print(f"  Run ID: {run_id}")
        return {"run_id": run_id, "metrics": metrics, "name": run_name}


def promote(client, version, run_id, force=False):
    """Promote only a strictly better model evaluated on identical validation data."""
    from mlflow.exceptions import MlflowException

    candidate = client.get_run(run_id)
    try:
        champion = client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, version)
        print("Promoted: no existing champion")
        return True
    previous = client.get_run(champion.run_id)
    keys = ("validation_sha256", "evaluation_method")
    comparable = all(
        candidate.data.tags.get(k)
        and candidate.data.tags.get(k) == previous.data.tags.get(k)
        for k in keys
    )
    old_f1 = previous.data.metrics.get("f1_score")
    new_f1 = candidate.data.metrics.get("f1_score")
    if force or (
        comparable and old_f1 is not None and new_f1 is not None and new_f1 > old_f1
    ):
        client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, version)
        print(
            f"Promoted version {version}; previous champion {champion.version} remains available for rollback"
        )
        return True
    print(
        "Champion retained: validation data or evaluation method differs, metric missing, or F1 did not improve"
    )
    return False


def main():
    import argparse

    from mlflow.exceptions import MlflowException

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff", type=Path, default=HANDOFF_PATH)
    parser.add_argument(
        "--force-promotion",
        action="store_true",
        help="Explicitly override champion comparison",
    )
    args = parser.parse_args()
    if not args.handoff.is_file():
        raise FileNotFoundError(
            f"Handoff not found: {args.handoff}. Run data preparation and copy outputs first."
        )
    contract = json.loads(args.handoff.read_text(encoding="utf-8"))
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment(EXPERIMENT_NAME)
    with mlflow.start_run(run_name="training-session"):
        frames = load_and_log_inputs(args.handoff)
    train_df, val_df = frames["train"], frames["validation"]
    if set(train_df.label) != {0, 1} or set(val_df.label) != {0, 1}:
        raise ValueError("Training and validation splits must contain both classes")
    configs = {
        "baseline": (
            "baseline_tfidf_lr",
            {"max_features": 5000},
            LogisticRegression(random_state=42, max_iter=1000),
        ),
        "improved": (
            "improved_tfidf_lr_bigrams",
            {"max_features": 10000, "ngram_range": (1, 2), "stop_words": "english"},
            LogisticRegression(random_state=42, max_iter=1000),
        ),
        "rf": (
            "random_forest_tfidf",
            {"max_features": 5000},
            RandomForestClassifier(n_estimators=100, random_state=42),
        ),
    }
    results = {
        name: run_experiment(*config, train_df, val_df, contract)
        for name, config in configs.items()
    }
    best_name, best = max(
        results.items(), key=lambda item: item[1]["metrics"]["f1_score"]
    )
    client = mlflow.tracking.MlflowClient()
    try:
        client.create_registered_model(MODEL_NAME)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_ALREADY_EXISTS":
            raise
    registered = client.create_model_version(
        name=MODEL_NAME,
        source=f"runs:/{best['run_id']}/model",
        run_id=best["run_id"],
        description=f"Best run: {best_name}",
    )
    promoted = promote(client, registered.version, best["run_id"], args.force_promotion)
    summary_path = Path("outputs/training_summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "registered_version": registered.version,
                "promoted": promoted,
                "best": best_name,
                "all_runs": results,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
