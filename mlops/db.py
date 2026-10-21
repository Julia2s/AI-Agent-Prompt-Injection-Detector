"""Atomic result writes and persistent failure counters (PostgreSQL)."""

from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TaskResult(Base):
    __tablename__ = "task_results"
    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    text: Mapped[str] = mapped_column(Text)
    is_injection: Mapped[bool] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(String, default="SUCCESS")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )


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
                # Serialize first-time schema creation when two workers start together.
                await connection.execute(text("SELECT pg_advisory_xact_lock(730219)"))
            await connection.run_sync(Base.metadata.create_all)

    async def close(self):
        await self.engine.dispose()

    async def get(self, task_id):
        async with self.sessions() as session:
            return await session.scalar(
                select(TaskResult).where(TaskResult.task_id == task_id)
            )

    async def save(self, task_id, text, prediction):
        async with self.sessions.begin() as session:
            await session.execute(
                self.insert(TaskResult)
                .values(
                    task_id=task_id,
                    text=text,
                    is_injection=prediction,
                )
                .on_conflict_do_nothing(index_elements=[TaskResult.task_id])
            )

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
