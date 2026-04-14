"""Database connection and session management."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from arb.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.db_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            json_serializer=lambda o: __import__("orjson").dumps(o).decode(),
            json_deserializer=lambda s: __import__("orjson").loads(s),
        )
    return _engine


def get_session_factory() -> async_sessionmaker:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def dispose_engine() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None
