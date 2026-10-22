"""python -m mlops.pipeline: validate, split, analyse and log data to MLflow."""

import argparse
import json
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import pandas as pd

from mlops.data import ROOT, load_raw, prepare, sha256

EXPERIMENT = "prompt-injection"
DATASET = "deepset-prompt-injections"


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def eda(splits: dict, stats: dict, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    train = splits["train"]
    lengths = train.text.str.len()
    words = train.text.str.split().str.len()
    summary = pd.DataFrame(
        {"label": train.label, "characters": lengths, "words": words}
    )
    summary.groupby("label")[["characters", "words"]].describe().to_csv(
        directory / "length_statistics.csv"
    )
    write_json(directory / "quality.json", stats)
    fig, ax = plt.subplots(figsize=(7, 4))
    pd.DataFrame(
        {name: frame.label.value_counts() for name, frame in splits.items()}
    ).T.plot(kind="bar", ax=ax, color=["#2878a5", "#d36a44"], rot=0)
    ax.set(title="Class balance by split", ylabel="Prompts", xlabel="Split")
    ax.legend(["0: benign", "1: injection"])
    fig.tight_layout()
    fig.savefig(directory / "class_balance.png", dpi=150)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(8, 4))
    for label in (0, 1):
        ax.hist(
            lengths[train.label == label],
            bins=[i * lengths.max() / 30 for i in range(31)],
            alpha=0.6,
            label=str(label),
        )
    ax.set(title="Training text length", xlabel="Characters", ylabel="Prompts")
    ax.legend(title="Label")
    fig.tight_layout()
    fig.savefig(directory / "text_lengths.png", dpi=150)
    plt.close(fig)


def run(tracking_uri: str, output: Path, raw_dir: Path = ROOT / "data/raw") -> dict:
    raw, source = load_raw(raw_dir)
    splits, stats = prepare(raw)
    output.mkdir(parents=True, exist_ok=True)
    data_dir = output / "data"
    data_dir.mkdir(exist_ok=True)
    for name, frame in splits.items():
        frame.to_csv(data_dir / f"{name}.csv", index=False, lineterminator="\n")
    eda(splits, stats, output / "eda")
    write_json(output / "source.json", source)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="infrastructure-data-eda") as active:
        mlflow.set_tags(
            {"stage": "data-preparation", "dataset.revision": source["revision"]}
        )
        mlflow.log_params({"seed": 42, "validation_fraction": 0.2, "label_positive": 1})
        mlflow.log_metrics(
            {"rows_raw": len(raw), **{f"rows_{k}": len(v) for k, v in splits.items()}}
        )
        mlflow.log_artifacts(str(raw_dir), artifact_path="raw")
        mlflow.log_artifacts(str(data_dir), artifact_path="data")
        mlflow.log_artifacts(str(output / "eda"), artifact_path="eda")
        mlflow.log_artifact(str(output / "source.json"))
        datasets = {}
        raw_dataset = mlflow.data.from_pandas(
            raw[["text", "label"]],
            source=f"https://huggingface.co/datasets/{source['repository']}/tree/{source['revision']}",
            targets="label",
            name=f"{DATASET}-raw",
        )
        mlflow.log_input(raw_dataset, context="source")
        for name, frame in splits.items():
            uri = f"runs:/{active.info.run_id}/data/{name}.csv"
            logged_frame = pd.read_csv(data_dir / f"{name}.csv", keep_default_na=False)
            dataset = mlflow.data.from_pandas(
                logged_frame,
                source=f"https://huggingface.co/datasets/{source['repository']}/tree/{source['revision']}",
                targets="label",
                name=f"{DATASET}-{name}",
            )
            mlflow.log_input(dataset, context=name)
            datasets[name] = {
                "name": dataset.name,
                "digest": dataset.digest,
                "source": f"https://huggingface.co/datasets/{source['repository']}/tree/{source['revision']}",
                "artifact_uri": uri,
                "sha256": sha256(data_dir / f"{name}.csv"),
                "rows": len(frame),
            }
        contract = {
            "experiment": EXPERIMENT,
            "data_run_id": active.info.run_id,
            "tracking_uri": tracking_uri,
            "tracking_uri_host": "http://localhost:5000",
            "tracking_uri_compose": "http://mlflow:5000",
            "source_revision": source["revision"],
            "datasets": datasets,
            "columns": {
                "text_id": "deduplication identity; NOT a model feature",
                "text": "input string",
                "label": "0=benign, 1=injection",
            },
            "model_name_for_participant_2": "prompt-injection-detector",
            "alias_for_participant_2": "champion",
            "model_registered": False,
        }
        write_json(output / "handoff.json", contract)
        mlflow.log_artifact(str(output / "handoff.json"))
    return contract


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tracking-uri",
        default=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"),
    )
    parser.add_argument("--output", type=Path, default=ROOT / "outputs")
    args = parser.parse_args()
    print(json.dumps(run(args.tracking_uri, args.output), indent=2))


if __name__ == "__main__":
    main()
