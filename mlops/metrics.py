"""Tiny dependency-free helpers for the experiment (source-side metrics)."""

import os
import time
from dataclasses import dataclass


def rss_mb() -> float:
    """Resident set size of THIS process in MiB (Linux; 0.0 elsewhere)."""
    try:
        with open("/proc/self/statm") as stream:
            pages = int(stream.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") / 1024 / 1024
    except (OSError, ValueError, IndexError):
        return 0.0


@dataclass
class SourceStats:
    """What the source itself observes. `buffered` is the number of tasks the source
    holds in ITS OWN memory waiting to be published: it must stay <= 1 under
    backpressure."""

    accepted: int = 0  # confirmed by the broker
    rejected: int = 0  # NACK/timeout -> retried later
    lost: int = 0  # only possible in the deliberately naive mode
    buffered: int = 0


SOURCE_COLUMNS = (
    "t",
    "accepted_total",
    "rejected_total",
    "lost_total",
    "accepted_per_s",
    "rejected_per_s",
    "buffered",
    "rss_mb",
)


class RateSampler:
    """Turns cumulative counters into per-second rates."""

    def __init__(self, stats: SourceStats, clock=time.monotonic):
        self.stats = stats
        self.clock = clock
        self.start = clock()
        self.last_time = self.start
        self.last_accepted = 0
        self.last_rejected = 0

    def sample(self) -> dict:
        now = self.clock()
        elapsed = max(now - self.last_time, 1e-9)
        row = {
            "t": round(now - self.start, 2),
            "accepted_total": self.stats.accepted,
            "rejected_total": self.stats.rejected,
            "lost_total": self.stats.lost,
            "accepted_per_s": round(
                (self.stats.accepted - self.last_accepted) / elapsed, 2
            ),
            "rejected_per_s": round(
                (self.stats.rejected - self.last_rejected) / elapsed, 2
            ),
            "buffered": self.stats.buffered,
            "rss_mb": round(rss_mb(), 1),
        }
        self.last_time = now
        self.last_accepted = self.stats.accepted
        self.last_rejected = self.stats.rejected
        return row


def csv_line(row: dict) -> str:
    return ",".join(str(row[column]) for column in SOURCE_COLUMNS)
