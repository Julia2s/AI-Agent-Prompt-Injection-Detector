"""Load immutable data and log the SAME inputs in the next participant's run."""

import json
from pathlib import Path

import mlflow
import pandas as pd

from mlops.data import sha256


def load_and_log_inputs(contract_path: str | Path) -> dict[str, pd.DataFrame]:
    """Call inside an active training run, with tracking URI already configured."""
    if mlflow.active_run() is None:
        raise RuntimeError("Call inside 'with mlflow.start_run()'")
    contract = json.loads(Path(contract_path).read_text())
    result = {}
    for split, info in contract["datasets"].items():
        path = Path(
            mlflow.artifacts.download_artifacts(artifact_uri=info["artifact_uri"])
        )
        if sha256(path) != info["sha256"]:
            raise ValueError(f"Artifact checksum mismatch: {split}")
        frame = pd.read_csv(path, keep_default_na=False)
        if len(frame) != info["rows"]:
            raise ValueError(f"Row count mismatch: {split}")
        if not {"text", "label"}.issubset(frame.columns):
            raise ValueError(f"Missing required columns: {split}")
        if (
            not frame["text"]
            .map(lambda text: isinstance(text, str) and bool(text.strip()))
            .all()
        ):
            raise ValueError(f"Invalid text: {split}")
        if not set(frame["label"]).issubset({0, 1}) or frame.empty:
            raise ValueError(f"Invalid labels: {split}")
        dataset = mlflow.data.from_pandas(
            frame, source=info["source"], targets="label", name=info["name"]
        )
        if dataset.digest != info["digest"]:
            raise ValueError(f"Dataset digest mismatch: {split}")
        mlflow.log_input(dataset, context=split)
        result[split] = frame
    mlflow.set_tag("data_run_id", contract["data_run_id"])
    return result
