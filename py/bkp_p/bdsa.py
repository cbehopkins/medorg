import asyncio
from datetime import datetime
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Iterable, Sequence, Union

from aiopath import AsyncPath
from sqlalchemy import (
    desc,
    select,
    delete,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from bkp_db.types import BackupDest, BackupFile, Base
from bkp_p.bkp_xml import BkpFile
from bkp_p.volume_id import VolumeId


class Bdsa:
    """Backup Database Syncing Asynchronously"""

    def __init__(self, session):
        self.session = session
        self._lock = asyncio.Lock()

    async def add_all(self, items):
        self.session.add_all(items)
        await self.session.commit()

    async def _query_dest_tag(self, dest_id: VolumeId) -> BackupDest:
        result = (
            (
                await self.session.execute(
                    select(BackupDest).filter(BackupDest.name == dest_id)
                )
            )
            .scalars()
            .all()
        )
        if not result:
            return BackupDest(name=dest_id)
        assert len(result) == 1
        assert isinstance(result[0], BackupDest)
        return result[0]

    async def init_dest(self, dest_id: VolumeId) -> None:
        """Initialise a destination
        Ensure a unique entry exists for the destination id

        Args:
            dest_id (VolumeId): The destination ID
        """
        result = (
            (
                await self.session.execute(
                    select(BackupDest).filter(BackupDest.name == dest_id)
                )
            )
            .scalars()
            .all()
        )
        if not result:
            result = BackupDest(name=dest_id)
            self.session.add(result)
            await self.session.commit()

    async def add_new_file(
        self,
        file_name: str,
        dest_names: Union[VolumeId, list[VolumeId]] = [""],
        size: int = 0,
        timestamp: Union[int] = 0,
        md5_hash: str = "",
        visited: bool = False,
    ) -> None:
        # This should only really be used in testbenches to setup files...
        if isinstance(timestamp, int):
            timestamp = datetime.fromtimestamp(timestamp)
        # Add a new file with a specified tag
        new_file = BackupFile(
            filename=file_name,
            size=size,
            timestamp=timestamp,
            md5_hash=md5_hash,
            visited=int(visited),
        )
        if dest_names:
            if not isinstance(dest_names, list):
                assert isinstance(dest_names, str)
                dest_names = [dest_names]
            await self.add_bkp_dests_to_backup_file(dest_names, new_file)

        self.write_file_entry(new_file)

    async def add_bkp_dests_to_backup_file(
        self, dest_names: Sequence[str], backup_file: BackupFile
    ) -> None:
        for dest_name in dest_names:
            new_tag = await self._query_dest_tag(dest_name)
            self.add_tag_obj_to_backup_file(new_tag, backup_file)

    @staticmethod
    def add_tag_obj_to_backup_file(new_tag, backup_file: BackupFile) -> None:
        # FIXME write test to make sure this comparison works as intended
        if new_tag.name not in backup_file.dest_names:
            backup_file.backup_dest.append(new_tag)
            backup_file.visited = 1

    def write_file_entry(self, entry: BackupFile) -> None:
        self.session.add(entry)

    async def query_all_files(self) -> list[BackupFile]:
        async with self._lock:
            result = await self.session.execute(select(BackupFile))
        return result.scalars().all()

    async def aquery_all_files(self) -> AsyncGenerator[BackupFile, None]:
        async with self._lock:
            result = await self.session.execute(select(BackupFile))
        for file in result.scalars():
            yield file

    async def query_filename(self, name: str) -> BackupFile:
        async with self._lock:
            query = await self.session.execute(
                select(BackupFile).filter(BackupFile.filename == name)
            )
        result = query.scalars().all()
        if not result:
            return None
        assert len(result) == 1
        return result[0]

    async def query_hash(self, hash: str) -> BackupFile:
        async with self._lock:
            query = await self.session.execute(
                select(BackupFile).filter(BackupFile.md5_hash == hash)
            )
        result = query.scalars().all()
        if not result:
            return None
        return result

    async def aquery_hash(self, hash: str) -> AsyncGenerator[BackupFile, None]:
        async with self._lock:
            query = await self.session.execute(
                select(BackupFile).filter(BackupFile.md5_hash == hash)
            )
        for file in query.scalars():
            yield file

    async def query_dest(self, dest_name: VolumeId) -> list[BackupFile]:
        query = select(BackupFile).filter(
            BackupFile.backup_dest.any(BackupDest.name == dest_name)
        )
        result = await self.session.execute(query)
        files = result.scalars().all()
        return files

    async def query_files_without_dest(self, dest_name: VolumeId) -> list[BackupFile]:
        # Query for files without a specified dest
        async with self._lock:
            result = (
                (
                    await self.session.execute(
                        select(BackupFile).filter(
                            ~BackupFile.backup_dest.any(BackupDest.name == dest_name)
                        )
                    )
                )
                .scalars()
                .all()
            )
        return result

    async def query_files_visited(self) -> list[BackupFile]:
        # Query for files That we visited this run
        async with self._lock:
            result = (
                (
                    await self.session.execute(
                        select(BackupFile).filter(BackupFile.visited != 0)
                    )
                )
                .scalars()
                .all()
            )
        return result

    async def for_backup(self, dest_name: VolumeId) -> list[BackupFile]:
        # Query for files without a specified dest
        # Ordered by largest files first
        async with self._lock:
            result = (
                (
                    await self.session.execute(
                        select(BackupFile).filter(
                            ~BackupFile.backup_dest.any(BackupDest.name == dest_name)
                        )
                        # FIXME we also need to order by fewest existing dest ids.
                        .order_by(desc(BackupFile.size))
                    )
                )
                .scalars()
                .all()
            )
        return result

    async def overwrite_file_entry_dest_ids(
        self, entry: BackupFile, dests: list[VolumeId]
    ) -> BackupFile:
        assert entry
        assert isinstance(dests, (list, set))
        entry.backup_dest.clear()
        fetched_tags = [await self._query_dest_tag(tag_name) for tag_name in set(dests)]
        entry.backup_dest.extend(fetched_tags)
        return entry

    async def update_file(self, props: BkpFile) -> BackupFile:
        """Update the database with the specified BkpFile Object

        Args:
            props (BkpFile): The properties of the file in question

        Returns:
            BackupFile: The entry from the database
        """

        entry = await self.query_filename(str(props.file_path))
        add_entry = entry is None
        if add_entry:
            entry = BackupFile(filename=str(props.file_path))
        await self.overwrite_file_entry_dest_ids(entry, props.bkp_dests)
        entry.size = props.size
        entry.md5_hash = props.md5
        entry.visited = 0
        entry.timestamp = datetime.fromtimestamp(props.mtime)
        if add_entry:
            self.write_file_entry(entry)
        return entry

    async def visit_files(self, props_src: Iterable[BkpFile]):
        """For each file property from the iterator, update the database

        Doing this will mark the entries as visited - which we may find interesting later...

        Args:
            props_src (Iterable[BkpFile]): A source of BkpFile(s)
        """
        # FIXME - onluy used in tests
        for props in props_src:
            entry = await self.update_file(props)
            entry.visited = 1

    @staticmethod
    def delete_unvisited_files(file: BackupFile) -> bool:
        # Return true if we want the file deleted
        visited = file.visited
        file.visited = 0
        return not visited

    async def filter(self, callback: Callable[[BackupFile], bool]):
        async for entry in self.aquery_all_files():
            if callback(entry):
                await self.session.delete(entry)

    async def discovery(self, files: list[BkpFile], dest: VolumeId):
        """Discovery is the process of discovering backed up files already on a dest

        Args:
            files (list[BkpFile]): The files you have found on the dest
            dest (VolumeId): The VolumeId that you found them on
        """
        # FIXME - make this more concurrent/able to read from a coroutibe
        tag_obj = await self._query_dest_tag(dest)

        for file in files:
            await self.discover_one_file(file, tag_obj)

    async def discover_one_file(self, file: BkpFile, tag_obj: BackupDest):
        async for result in self.aquery_hash(file.md5):
            self.add_tag_obj_to_backup_file(tag_obj, result)


class DatabaseHandler:
    def __init__(self, db_directory, *, echo=False):
        self.engine = None
        self.db_directory = db_directory
        self._db_path = None
        self.echo = echo

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session_scope(self):
        async with AsyncSession(self.engine) as session:
            yield Bdsa(session)
            await session.commit()

    @property
    async def db_path(self) -> AsyncPath:
        if self.db_directory is None:
            self.db_directory = AsyncPath.home() / ".bkp_base"
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
            sys.exit(1)
        if populate_tables:
            await self.create_tables()

    async def clear_files(self) -> None:
        """Sometimes we need to clear the current files, but keep everything else"""
        async with self.engine.begin() as conn:
            await conn.execute(delete(BackupFile))
