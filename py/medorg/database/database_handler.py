import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from aiopath import AsyncPath
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from medorg.common.types import BackupFile, Base
from medorg.database.bdsa import Bdsa


class DatabaseHandler:
    """Create the database in the providedd location

    Args:
        db_directory (Path): The directory to store the database in Defaults to usimg ~/.bkp_base
        echo (bool, optional): Echo the SQL commands. Defaults to False.
    """

    def __init__(self, db_directory: Path | None, *, echo=False):
        self.engine = None
        self.db_directory = db_directory
        self._db_path = None
        self.echo = echo

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session_scope(self) -> AsyncGenerator[Bdsa, None]:
        async with AsyncSession(self.engine) as session:
            bdsa = Bdsa(session, self.session_maker)
            yield bdsa
            await bdsa.clear_all_visited()
            await session.commit()

    def session_maker(self) -> AsyncSession:
        return AsyncSession(self.engine)

    @property
    async def db_path(self) -> AsyncPath:
        if self.db_directory is None:
            self.db_directory = await AsyncPath.home() / ".bkp_base"
        else:
            self.db_directory = AsyncPath(self.db_directory)

        await self.db_directory.mkdir(parents=True, exist_ok=True)
        self._db_path = self.db_directory / "backup_database.db"
        return self._db_path

    def _create_engine(self, db_url):
        self.engine = create_async_engine(
            db_url,
            echo=self.echo,
        )

    async def create_session(self):
        db_path = await self.db_path
        populate_tables = not await db_path.is_file()
        try:
            self._create_engine(f"sqlite+aiosqlite:///{db_path}")
        except Exception as e:
            print(f"Error creating database: {e}")
            # FIXME Exceptionize this
            sys.exit(1)
        if populate_tables:
            await self.create_tables()

    async def clear_files(self) -> None:
        """Sometimes we need to clear the current files, but keep everything else"""
        async with self.engine.begin() as conn:
            await conn.execute(delete(BackupFile))
