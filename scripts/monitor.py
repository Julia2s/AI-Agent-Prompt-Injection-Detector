"""Experiment observer (NOT part of the control loop).

Samples every --interval seconds:
  * queue depth (ready / unacked), consumers, accepted-publish rate and ack rate,
    read from the RabbitMQ Management API;
  * memory (MiB) of docker containers matching --match NAME (summed over replicas)
    and/or of local processes given as --pid NAME=PID.

The pipeline itself never polls the Management API: the source learns about
overload from publisher confirms. This script only RECORDS what happens, which is
why using the API here does not contradict the design.

stdlib only, so it runs on the host without the project virtualenv:
    python3 scripts/monitor.py --queue inference_queue_v2 --out runs/x/monitor.csv \
        --match worker --match rabbitmq --duration 120
"""

import argparse
import base64
import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

UNITS = {
    "b": 1 / 1024 / 1024,
    "kib": 1 / 1024,
    "kb": 1 / 1024,
    "mib": 1.0,
    "mb": 1.0,
    "gib": 1024.0,
    "gb": 1024.0,
}


def parse_mem(text: str) -> float:
    """'123.4MiB / 1GiB' -> 123.4 (MiB). Only the usage before '/' counts."""
    match = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text.split("/")[0])
    if not match:
        return 0.0
    return float(match.group(1)) * UNITS.get(match.group(2).lower(), 0.0)


def queue_row(current: dict, previous: dict | None, dt: float) -> dict:
    stats = current.get("message_stats", {})
    before = (previous or {}).get("message_stats", {})
    rate = lambda key: (  # noqa: E731
        round((stats.get(key, 0) - before.get(key, 0)) / dt, 2)
        if previous and dt > 0
        else 0.0
    )
    ready = current.get("messages_ready", 0)
    unacked = current.get("messages_unacknowledged", 0)
    return {
        "ready": ready,
        "unacked": unacked,
        "depth": ready + unacked,
        "consumers": current.get("consumers", 0),
        "publish_per_s": rate("publish"),
        "ack_per_s": rate("ack"),
    }


def fetch_queue(base: str, queue: str, user: str, password: str) -> dict | None:
    url = f"{base}/api/queues/%2F/{urllib.parse.quote(queue, safe='')}"
    request = urllib.request.Request(url)
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return None


def docker_memory(matches: list[str]) -> dict[str, float]:
    totals = dict.fromkeys(matches, 0.0)
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}},{{.MemUsage}}"],
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return totals
    for line in out.splitlines():
        name, _, usage = line.partition(",")
        for match in matches:
            if match in name:
                totals[match] += parse_mem(usage)
    return totals


def pid_memory(pids: dict[str, int]) -> dict[str, float]:
    result = {}
    for name, pid in pids.items():
        try:
            with open(f"/proc/{pid}/statm") as stream:
                pages = int(stream.read().split()[1])
            result[name] = pages * os.sysconf("SC_PAGE_SIZE") / 1024 / 1024
        except (OSError, ValueError, IndexError):
            result[name] = 0.0
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--mgmt", default="http://localhost:15672")
    parser.add_argument("--user", default="guest")
    parser.add_argument("--password", default="guest")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--match", action="append", default=[], help="docker name part")
    parser.add_argument("--pid", action="append", default=[], help="NAME=PID")
    args = parser.parse_args()

    pids = {}
    for item in args.pid:
        name, _, pid = item.partition("=")
        pids[name] = int(pid)
    memory_names = [*args.match, *pids]
    columns = [
        "t",
        "ready",
        "unacked",
        "depth",
        "consumers",
        "publish_per_s",
        "ack_per_s",
        *[f"mem_{name}_mb" for name in memory_names],
    ]
    start = time.monotonic()
    previous, previous_time = None, start
    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        while time.monotonic() - start < args.duration:
            tick = time.monotonic()
            current = fetch_queue(args.mgmt, args.queue, args.user, args.password)
            if current is not None:
                row = {"t": round(tick - start, 1)}
                row.update(queue_row(current, previous, tick - previous_time))
                memory = {
                    **(docker_memory(args.match) if args.match else {}),
                    **pid_memory(pids),
                }
                for name in memory_names:
                    row[f"mem_{name}_mb"] = round(memory.get(name, 0.0), 1)
                writer.writerow(row)
                handle.flush()
                print(
                    f"t={row['t']:>6} ready={row['ready']:>6} unacked={row['unacked']:>3} "
                    f"pub/s={row['publish_per_s']:>6} ack/s={row['ack_per_s']:>6}",
                    file=sys.stderr,
                )
                previous, previous_time = current, tick
            time.sleep(max(0.0, args.interval - (time.monotonic() - tick)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
