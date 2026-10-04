"""Verified source snapshot and deterministic leakage-safe data preparation."""

import hashlib
import json
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[1]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_raw(directory: Path) -> tuple[pd.DataFrame, dict]:
    source = json.loads((directory / "source.json").read_text())
    frames = []
    for name, metadata in source["files"].items():
        path = directory / name
        if sha256(path) != metadata["sha256"]:
            raise ValueError(f"Source checksum mismatch: {name}")
        if name.endswith(".parquet"):
            frame = pd.read_parquet(path)
            frame["original_split"] = name.split("-")[0]
            frames.append(frame)
    if not frames:
        raise ValueError("No parquet data in source manifest")
    return pd.concat(frames, ignore_index=True), source


def prepare(raw: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict]:
    required = {"text", "label", "original_split"}
    if not required.issubset(raw.columns):
        raise ValueError(f"Required columns: {sorted(required)}")
    df = raw.copy()
    if df[list(required)].isna().any().any():
        raise ValueError("Missing text, label or split")
    if not df["text"].map(lambda value: isinstance(value, str)).all():
        raise ValueError("All text values must be strings")
    if not set(df["label"]).issubset({0, 1}) or set(df["label"]) != {0, 1}:
        raise ValueError("Labels must contain both 0 and 1")
    if set(df["original_split"]) != {"train", "test"}:
        raise ValueError("Expected original train and test splits")

    df["key"] = df["text"].str.split().str.join(" ").str.casefold()
    if df["key"].eq("").any():
        raise ValueError("Empty text")
    conflicts = df.groupby("key")["label"].nunique()
    if (conflicts > 1).any():
        raise ValueError("Conflicting labels for normalized duplicate text")
    counts = df["original_split"].value_counts().to_dict()

    test = df[df.original_split == "test"].drop_duplicates("key")
    train = df[df.original_split == "train"].drop_duplicates("key")
    overlap = int(train["key"].isin(test["key"]).sum())
    train = train[~train["key"].isin(test["key"])]
    train, validation = train_test_split(
        train, test_size=0.2, random_state=42, stratify=train["label"]
    )
    splits = {}
    for name, frame in (("train", train), ("validation", validation), ("test", test)):
        frame = frame.copy()
        frame["text_id"] = frame["key"].map(
            lambda value: hashlib.sha256(value.encode()).hexdigest()
        )
        frame["label"] = frame["label"].astype("int64")
        splits[name] = (
            frame[["text_id", "text", "label"]]
            .sort_values("text_id")
            .reset_index(drop=True)
        )
    stats = {
        "raw_rows": len(raw),
        "original_splits": counts,
        "missing_cells": int(raw[list(required)].isna().sum().sum()),
        "duplicate_rows_normalized": int(df.duplicated("key").sum()),
        "train_test_overlap_removed": overlap,
        "clean_rows": sum(map(len, splits.values())),
        "splits": {name: len(frame) for name, frame in splits.items()},
        "class_counts": {
            name: {str(k): int(v) for k, v in frame.label.value_counts().items()}
            for name, frame in splits.items()
        },
    }
    return splits, stats
