import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from mlops.metrics import RateSampler, SourceStats, csv_line
from mlops.producer import rows, run_backpressure, run_naive
from mlops.settings import PipelineSettings
from scripts.monitor import parse_mem, queue_row


def test_rate_sampler_computes_per_second_rates():
    stats, now = SourceStats(), [0.0]
    sampler = RateSampler(stats, clock=lambda: now[0])
    stats.accepted, stats.rejected, now[0] = 10, 4, 2.0
    row = sampler.sample()
    assert row["accepted_per_s"] == 5 and row["rejected_per_s"] == 2
    stats.accepted, now[0] = 12, 3.0
    assert sampler.sample()["accepted_per_s"] == 2
    assert csv_line(row).startswith("2.0,10,4,0,5.0,2.0,0,")


def test_rows_are_stable_and_padding_is_optional(tmp_path):
    path = tmp_path / "in.csv"
    path.write_text("text\nhello\nworld\n", encoding="utf-8")
    first = [t["task_id"] for t in rows(path, "b")]
    assert first == [t["task_id"] for t in rows(path, "b")]
    assert first != [t["task_id"] for t in rows(path, "other")]
    assert "padding" not in next(rows(path, "b"))
    assert len(next(rows(path, "b", pad_bytes=100))["padding"]) == 100


async def test_backpressure_source_holds_one_task_and_loses_none():
    broker = Mock()
    broker.publish = AsyncMock(side_effect=[TimeoutError(), None, None])
    stats = SourceStats()
    tasks = ({"task_id": str(i), "text": "t"} for i in range(2))
    settings = PipelineSettings(retry_delay=0.001, source_backoff_max=0.002)
    await run_backpressure(broker, settings, tasks, stats, deadline=None)
    assert (stats.accepted, stats.rejected, stats.lost, stats.buffered) == (2, 1, 0, 0)


async def test_naive_source_buffers_everything_and_loses_rejected():
    broker = Mock()
    gate = asyncio.Event()

    async def slow_publish(*_, **__):
        await gate.wait()
        raise TimeoutError()

    broker.publish = slow_publish
    stats = SourceStats()
    tasks = ({"task_id": str(i), "text": "t"} for i in range(50))
    runner = asyncio.create_task(
        run_naive(broker, PipelineSettings(), tasks, stats, deadline=None)
    )
    await asyncio.sleep(0.05)
    assert stats.buffered == 50  # unbounded in-memory buffer
    gate.set()
    await runner
    assert stats.lost == 50 and stats.accepted == 0


@pytest.mark.parametrize(
    ("text", "expected"),
    [("123.4MiB / 1GiB", 123.4), ("1.5GiB / 4GiB", 1536.0), ("512KiB / 1GiB", 0.5)],
)
def test_parse_docker_memory(text, expected):
    assert parse_mem(text) == pytest.approx(expected)


def test_queue_row_derives_depth_and_rates():
    def sample(ready, unacked, publish, ack):
        return {
            "messages_ready": ready,
            "messages_unacknowledged": unacked,
            "consumers": 1,
            "message_stats": {"publish": publish, "ack": ack},
        }

    row = queue_row(sample(50, 1, 140, 100), sample(50, 1, 100, 80), dt=2.0)
    assert row["depth"] == 51 and row["publish_per_s"] == 20 and row["ack_per_s"] == 10
