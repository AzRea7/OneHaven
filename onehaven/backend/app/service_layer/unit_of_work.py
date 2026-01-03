# app/service_layer/unit_of_work.py
from __future__ import annotations

from typing import Protocol
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import async_session
from ..adapters.repos.sqlalchemy_repos import SqlAlchemyRepos


class UnitOfWork(Protocol):
    repos: SqlAlchemyRepos

    async def __aenter__(self) -> "UnitOfWork": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class SqlAlchemyUnitOfWork:
    def __init__(self) -> None:
        self.session: AsyncSession | None = None
        self.repos: SqlAlchemyRepos | None = None

    async def __aenter__(self) -> "SqlAlchemyUnitOfWork":
        self.session = async_session()
        self.repos = SqlAlchemyRepos(self.session)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc:
                await self.rollback()
            else:
                await self.commit()
        finally:
            if self.session:
                await self.session.close()

    async def commit(self) -> None:
        assert self.session is not None
        await self.session.commit()

    async def rollback(self) -> None:
        assert self.session is not None
        await self.session.rollback()
