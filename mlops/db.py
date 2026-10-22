"""Atomic result writes and persistent failure counters (PostgreSQL)."""

from datetime import UTC, datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    delete,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

PENDING = "PENDING"
SUCCESS = "SUCCESS"

# Idempotent upgrades for databases created by the previous schema version
# (create_all never alters existing tables).
POSTGRES_UPGRADES = (
    "ALTER TABLE task_results ALTER COLUMN is_injection DROP NOT NULL",
    "ALTER TABLE task_results ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ",
    "ALTER TABLE task_results ADD COLUMN IF NOT EXISTS inference_ms DOUBLE PRECISION",
    "ALTER TABLE task_results ADD COLUMN IF NOT EXISTS latency_ms DOUBLE PRECISION",
)


def utcnow():
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class TaskResult(Base):
    __tablename__ = "task_results"
    id: Mapped[int] = mapped_column(primary_key=True)
    # UNIQUE(task_id) is what makes processing idempotent.
    task_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    text: Mapped[str] = mapped_column(Text)
    # NULL while the task is PENDING.
    is_injection: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    status: Mapped[str] = mapped_column(String, default=SUCCESS)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Time spent inside the model call (what INFERENCE_DELAY controls).
    inference_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    # created_at of the message -> result written: queueing + inference.
    latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)


class Failure(Base):
    __tablename__ = "task_failures"
    message_key: Mapped[str] = mapped_column(String, primary_key=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)


class ResultStore:
    def __init__(self, url):
        self.engine = create_async_engine(url)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.insert = sqlite_insert if url.startswith("sqlite") else pg_insert

    async def initialize(self):
        async with self.engine.begin() as connection:
            if connection.dialect.name == "postgresql":
                # Serialize first-time schema creation when several processes
                # (API + workers) start together.
                await connection.execute(text("SELECT pg_advisory_xact_lock(730219)"))
            await connection.run_sync(Base.metadata.create_all)
            if connection.dialect.name == "postgresql":
                for statement in POSTGRES_UPGRADES:
                    await connection.execute(text(statement))

    async def close(self):
        await self.engine.dispose()

    async def get(self, task_id):
        async with self.sessions() as session:
            return await session.scalar(
                select(TaskResult).where(TaskResult.task_id == task_id)
            )

    async def create_pending(self, task_id, task_text):
        """Register an accepted-for-publication task. Returns (row, created)."""
        async with self.sessions.begin() as session:
            result = await session.execute(
                self.insert(TaskResult)
                .values(task_id=task_id, text=task_text, status=PENDING)
                .on_conflict_do_nothing(index_elements=[TaskResult.task_id])
            )
            created = result.rowcount == 1
        return await self.get(task_id), created

    async def discard_pending(self, task_id):
        """Compensation when publication failed: never leave a PENDING ghost."""
        async with self.sessions.begin() as session:
            await session.execute(
                delete(TaskResult).where(
                    TaskResult.task_id == task_id, TaskResult.status == PENDING
                )
            )

    async def save(
        self, task_id, task_text, prediction, inference_ms=None, latency_ms=None
    ):
        """Write the result once. Atomic upsert: PENDING -> SUCCESS, a row that is
        already SUCCESS is never touched, so redelivery cannot duplicate or
        overwrite a result."""
        now = utcnow()
        statement = self.insert(TaskResult).values(
            task_id=task_id,
            text=task_text,
            is_injection=prediction,
            status=SUCCESS,
            finished_at=now,
            inference_ms=inference_ms,
            latency_ms=latency_ms,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[TaskResult.task_id],
            set_={
                "is_injection": prediction,
                "status": SUCCESS,
                "finished_at": now,
                "inference_ms": inference_ms,
                "latency_ms": latency_ms,
            },
            where=TaskResult.status == PENDING,
        )
        async with self.sessions.begin() as session:
            await session.execute(statement)

    async def failures(self, key):
        async with self.sessions() as session:
            return (
                await session.scalar(
                    select(Failure.attempts).where(Failure.message_key == key)
                )
            ) or 0

    async def record_failure(self, key):
        async with self.sessions.begin() as session:
            stmt = self.insert(Failure).values(message_key=key, attempts=1)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Failure.message_key],
                set_={"attempts": Failure.attempts + 1},
            ).returning(Failure.attempts)
            return (await session.execute(stmt)).scalar_one()
