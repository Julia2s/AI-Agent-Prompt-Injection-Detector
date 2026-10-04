from pathlib import Path

import pandas as pd
import pytest

from mlops.data import ROOT, load_raw, prepare
from mlops.handoff import load_and_log_inputs
from mlops.pipeline import run


def test_real_snapshot_is_valid_and_disjoint():
    raw, source = load_raw(ROOT / "data/raw")
    assert len(source["revision"]) == 40
    first, stats = prepare(raw)
    second, _ = prepare(raw)
    assert stats["raw_rows"] == 662
    for split, frame in first.items():
        pd.testing.assert_frame_equal(frame, second[split])
        assert set(frame.label) == {0, 1}
    for a, b in (("train", "validation"), ("train", "test"), ("validation", "test")):
        assert set(first[a].text_id).isdisjoint(first[b].text_id)


@pytest.mark.parametrize("case", ["missing", "empty", "label", "conflict"])
def test_reject_bad_input(case):
    raw, _ = load_raw(ROOT / "data/raw")
    if case == "missing":
        raw.loc[0, "text"] = None
    elif case == "empty":
        raw.loc[0, "text"] = "  "
    elif case == "label":
        raw.loc[0, "label"] = 9
    else:
        extra = raw.iloc[[0]].copy()
        extra["label"] = 1 - extra.label
        raw = pd.concat([raw, extra])
    with pytest.raises(ValueError):
        prepare(raw)


def test_checksum_detects_tampering(tmp_path):
    import shutil

    shutil.copytree(ROOT / "data/raw", tmp_path / "raw")
    file = next((tmp_path / "raw").glob("*.parquet"))
    file.write_bytes(file.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="checksum"):
        load_raw(tmp_path / "raw")


def test_real_mlflow_roundtrip(tmp_path):
    import mlflow
    from mlflow import MlflowClient

    # Isolated SQL backend; production compose uses PostgreSQL.
    uri = f"sqlite:///{tmp_path / 'tracking.db'}"
    mlflow.set_tracking_uri(uri)
    client = MlflowClient(tracking_uri=uri)
    client.create_experiment(
        "prompt-injection", artifact_location=str(tmp_path / "artifacts")
    )
    output = tmp_path / "outputs"
    contract = run(uri, output)
    saved = client.get_run(contract["data_run_id"])
    assert saved.info.status == "FINISHED"
    assert len(saved.inputs.dataset_inputs) == 4
    assert {p.path for p in client.list_artifacts(saved.info.run_id, "eda")} >= {
        "eda/EDA.md",
        "eda/class_balance.png",
        "eda/text_lengths.png",
    }
    with mlflow.start_run(run_name="handoff-verification") as active:
        frames = load_and_log_inputs(output / "handoff.json")
        next_run = active.info.run_id
    assert len(frames["train"]) == contract["datasets"]["train"]["rows"]
    assert len(client.get_run(next_run).inputs.dataset_inputs) == 3
    assert Path(output / "handoff.json").exists()
