"""Build comparison plots and a summary table from experiment runs.

    uv run python scripts/report.py \
        --run "no limits, 1 worker=runs/unbounded-1w" \
        --run "backpressure, 1 worker=runs/bounded-1w" \
        --run "backpressure, 2 workers=runs/bounded-2w" \
        --limit 50 --out runs/report

Each run directory may contain:
    monitor.csv   (scripts/monitor.py)  queue depth, broker-side rates, memory
    source.csv    (producer metrics)    source-side rate, buffer, RSS, lost tasks
    latency.csv   (DB export)           per-task inference_ms / latency_ms
"""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def load(directory: Path, name: str) -> pd.DataFrame | None:
    path = directory / name
    if not path.exists() or path.stat().st_size == 0:
        return None
    frame = pd.read_csv(path)
    return frame if len(frame) else None


def smooth(series: pd.Series, window: int = 5) -> pd.Series:
    return series.rolling(window, min_periods=1, center=True).mean()


def steady_slice(monitor: pd.DataFrame, source: pd.DataFrame | None) -> pd.DataFrame:
    """Second half of the period in which the source was running.

    Samples where the Management API returned stale statistics (both rates are 0
    although the queue is not empty) are dropped: they are an artifact of the
    broker's statistics refresh interval, not real stalls."""
    active = (
        float(source["t"].max()) if source is not None else float(monitor["t"].max())
    )
    window = monitor[(monitor["t"] >= active / 2) & (monitor["t"] <= active)]
    stale = (window["publish_per_s"] == 0) & (window["ack_per_s"] == 0)
    return window[~stale]


def summarize(label: str, runs: dict) -> dict:
    monitor, source, latency = runs["monitor"], runs["source"], runs["latency"]
    row = {"run": label}
    if monitor is not None:
        steady = steady_slice(monitor, source)
        row["max queue ready"] = int(monitor["ready"].max())
        row["max ready+unacked"] = int(monitor["depth"].max())
        # NB: the broker's `publish` counter also counts publishes it rejected,
        # so it is "attempts/s". The accepted rate comes from the source below.
        row["publish attempts/s at broker (steady)"] = round(
            steady["publish_per_s"].mean(), 2
        )
        row["worker ack/s (steady)"] = round(steady["ack_per_s"].mean(), 2)
        for column in monitor.columns:
            if column.startswith("mem_"):
                row[f"max {column}"] = round(monitor[column].max(), 1)
    if source is not None:
        half = source[source["t"] >= source["t"].max() / 2]
        row["SOURCE accepted tasks/s (steady)"] = round(
            half["accepted_per_s"].mean(), 2
        )
        row["source max buffered tasks"] = int(source["buffered"].max())
        row["source max RSS, MiB"] = round(source["rss_mb"].max(), 1)
        row["rejected publish attempts"] = int(source["rejected_total"].max())
        row["LOST tasks"] = int(source["lost_total"].max())
    if latency is not None:
        # Latency covers only tasks that FINISHED; tasks still queued are not counted.
        row["tasks finished"] = len(latency)
        row["inference ms (mean)"] = round(latency["inference_ms"].mean(), 1)
        row["latency s p50"] = round(latency["latency_ms"].quantile(0.5) / 1000, 2)
        row["latency s p95"] = round(latency["latency_ms"].quantile(0.95) / 1000, 2)
        row["latency s max"] = round(latency["latency_ms"].max() / 1000, 2)
    return row


def to_markdown(table: pd.DataFrame) -> str:
    """Metrics as rows, runs as columns (no `tabulate` dependency)."""
    labels = list(table["run"])
    lines = ["| metric | " + " | ".join(labels) + " |", "|---|" + "---|" * len(labels)]
    for column in table.columns[1:]:
        cells = ["" if pd.isna(v) else str(v) for v in table[column]]
        lines.append(f"| {column} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def line_plot(runs, title, ylabel, getter, path, hline=None, log=False):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    drawn = False
    for label, data in runs.items():
        series = getter(data)
        if series is None:
            continue
        x, y = series
        ax.plot(x, y, label=label, linewidth=1.6)
        drawn = True
    if hline:
        ax.axhline(hline[0], color="red", linestyle="--", linewidth=1, label=hline[1])
    ax.set(title=title, xlabel="seconds since start", ylabel=ylabel)
    if log:
        ax.set_yscale("log")
    ax.grid(alpha=0.3)
    if drawn:
        ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="append", required=True, help="LABEL=DIR")
    parser.add_argument("--limit", type=int, default=50, help="queue limit to draw")
    parser.add_argument("--out", default="runs/report")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    runs = {}
    for item in args.run:
        label, _, directory = item.rpartition("=")
        folder = Path(directory)
        runs[label] = {
            "monitor": load(folder, "monitor.csv"),
            "source": load(folder, "source.csv"),
            "latency": load(folder, "latency.csv"),
        }

    def depth(data):
        m = data["monitor"]
        return None if m is None else (m["t"], m["depth"].clip(lower=1))

    line_plot(
        runs,
        "Messages held by the broker: ready + unacked",
        "messages",
        depth,
        out / "queue_depth.png",
        log=True,
    )

    def ready(data):
        m = data["monitor"]
        return None if m is None else (m["t"], m["ready"].clip(lower=1))

    line_plot(
        runs,
        "Queue length (messages_ready) vs x-max-length",
        "messages",
        ready,
        out / "queue_ready.png",
        hline=(args.limit, f"x-max-length = {args.limit}"),
        log=True,
    )

    def source_rate(data):
        s = data["source"]
        return None if s is None else (s["t"], smooth(s["accepted_per_s"]))

    line_plot(
        runs,
        "Source rate: tasks accepted by the broker per second",
        "tasks/s",
        source_rate,
        out / "source_rate.png",
        log=True,
    )

    def consumer_rate(data):
        m = data["monitor"]
        return None if m is None else (m["t"], smooth(m["ack_per_s"]))

    line_plot(
        runs,
        "Consumption rate: acks per second (all workers)",
        "tasks/s",
        consumer_rate,
        out / "worker_rate.png",
    )

    memory_columns = sorted(
        {
            c
            for d in runs.values()
            if d["monitor"] is not None
            for c in d["monitor"].columns
            if c.startswith("mem_")
        }
    )
    for column in memory_columns:

        def memory(data, column=column):
            m = data["monitor"]
            return None if m is None or column not in m else (m["t"], m[column])

        line_plot(runs, column.replace("_", " "), "MiB", memory, out / f"{column}.png")

    def source_memory(data):
        s = data["source"]
        return None if s is None else (s["t"], s["rss_mb"])

    line_plot(runs, "Source process RSS", "MiB", source_memory, out / "source_rss.png")

    def source_buffer(data):
        s = data["source"]
        return None if s is None else (s["t"], s["buffered"].clip(lower=0.9))

    line_plot(
        runs,
        "Tasks held in the SOURCE's own memory",
        "tasks",
        source_buffer,
        out / "source_buffer.png",
        log=True,
    )

    def latency(data):
        frame = data["latency"]
        if frame is None:
            return None
        t = pd.to_datetime(frame["finished_at"], format="mixed")
        return (t - t.min()).dt.total_seconds(), frame["latency_ms"] / 1000

    line_plot(
        runs,
        "End-to-end task latency (created -> result stored)",
        "seconds",
        latency,
        out / "latency.png",
        log=True,
    )

    table = pd.DataFrame([summarize(label, data) for label, data in runs.items()])
    markdown = to_markdown(table)
    (out / "summary.md").write_text(markdown + "\n", encoding="utf-8")
    table.to_csv(out / "summary.csv", index=False)
    print(markdown)


if __name__ == "__main__":
    main()
