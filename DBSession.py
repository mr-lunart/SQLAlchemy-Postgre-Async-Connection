import os
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

load_dotenv()

def create_url(
    username: str,
    password: str,
    host: str,
    database: str,
    port: str,
    psycopg_version: int = 3,
) -> URL:
    drivers = {
        3: "postgresql+psycopg",
        2: "postgresql+psycopg2",
    }
    if psycopg_version not in drivers:
        raise ValueError(f"Unsupported psycopg_version: {psycopg_version}. Use 2 or 3.")

    return URL.create(
        drivers[psycopg_version],
        username=username,
        password=password,
        host=host,
        database=database,
        port=port,
    )

class DBSession:
    def __init__(self, database_url: str):
        """
        Initialize an async session pool for PostgreSQL.

        Connection Templates:
            psycopg v3 (recommended): postgresql+psycopg://{user}:{pass}@{host}:{port}/{db}
            psycopg v2              : postgresql+psycopg2://{user}:{pass}@{host}:{port}/{db}

        Args:
            database_url: Full SQLAlchemy-compatible async connection string.
        """
        self._engine: AsyncEngine = create_async_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=70,       # Increase pool size to handle more connections
            max_overflow=30,    # Allow for more overflow connections
            pool_recycle=1800,  # recycle connections every 30 min
            pool_timeout=60,    # timeout for acquiring connections, activate when total conn over pool_size + overflow
            connect_args={
                "connect_timeout": 120,             # 120 seconds to establish connection
                "application_name": "standard",
            },
        )

        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """
            Async generator

            Usage:
            db_session = DBSession(url)

            async with db_session.session() as conn:
        """
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            finally:
                await session.close()

    async def get_db(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Async generator for use with FastAPI's Depends() or async with.

        Usage:
            db_session = DBSession(url)

            @app.get("/items")
            async def read_items(db: AsyncSession = Depends(db_session.get_db)):
                ...

        Usage (script):
            async for db in db_session.get_db():
                result = await db.execute(...)
        """
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

db_url = create_url(
    username=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    port=os.getenv("DB_PORT", "5432"),
)

SESSION_MAKER = DBSession(db_url)