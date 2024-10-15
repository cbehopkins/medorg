import asyncio
from collections import Counter, defaultdict
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Awaitable, Callable, Iterable, Sequence

from aiopath import AsyncPath
from sqlalchemy import desc, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import BinaryExpression, Select, and_

from medorg.common.bkp_file import BkpFile
from medorg.common.types import (BackupDest, BackupFile, BackupSrc,
                                 DatabaseBase, VolumeId)
from medorg.restore.structs import RestoreContext, RestoreDirectory

_log = logging.getLogger(__name__)


class AsyncSessionWrapper:

    def __init__(
        self, session: AsyncSession, session_maker: Callable[[None], AsyncSession]
    ) -> None:
        self.session = session
        self.session_maker = session_maker
        self._lock = asyncio.Lock()

    async def aquery_generator(
        self,
        select_type: DatabaseBase,
        filter_option: BinaryExpression | None = None,
        order_by=None,
        skip_lock=False,
    ) -> Iterable[DatabaseBase]:
        async def _run_query(select_type, filter_option, order_by):
            statement: Select = select(select_type)
            if filter_option is not None:
                statement = statement.filter(filter_option)
            if order_by is not None:
                statement = statement.order_by(order_by)
            result = await self.session.execute(statement)
            return result

        if skip_lock:
            result = await _run_query(select_type, filter_option, order_by)
        else:
            async with self._lock:
                result = await _run_query(select_type, filter_option, order_by)
        return result.unique().scalars()

    async def aquery_one(
        self, select_type: DatabaseBase, filter_option: BinaryExpression
    ) -> DatabaseBase:
        async with self._lock:
            if filter_option is None:
                statement: Select = select(select_type)
            else:
                statement: Select = select(select_type).filter(filter_option)
            result = await self.session.execute(statement)
        return result.scalar()

    async def aquery_many(
        self,
        select_type: DatabaseBase,
        filter_options: list[tuple[BinaryExpression, Any]],
        on_fail: Callable[[Any], DatabaseBase] = None,
    ) -> list[DatabaseBase | None]:
        def handle_fail(result: DatabaseBase | None, val: Any) -> DatabaseBase | None:
            if result is None:
                return on_fail(val) if on_fail else None
            return result

        async def my_task(my_session, statement, val: Any) -> DatabaseBase | None:
            async with self._lock:
                result = await my_session.execute(statement)
            return handle_fail(result.scalar(), val)

        # FIXME - have each query with own session
        # Requires the current session to be comitted
        # Which invalidates current references
        # So yeah! Bad times...
        tasks = []
        for filter_option, val in filter_options:
            if filter_option is None:
                statement: Select = select(select_type)
            else:
                statement: Select = select(select_type).filter(filter_option)
            task = my_task(self.session, statement, val)
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results
    # For test mocking
    def _session_add(self, obj: DatabaseBase):
        self.session.add(obj)

    async def add(self, obj: DatabaseBase):
        async with self._lock:
            self._session_add(obj)
            await self.session.commit()

    async def filter(
        self, callback: Callable[[DatabaseBase], bool], select_type: DatabaseBase
    ):
        async with self._lock:
            for entry in await self.aquery_generator(select_type, skip_lock=True):
                if callback(entry):
                    await self.session.delete(entry)


class Bdsa(AsyncSessionWrapper):
    """Backup Database Syncing Asynchronously"""

    async def add_src_dir(self, src_dir: Path) -> None:
        """Add a source directory to the database

        Args:
            src_dir (Path): The source directory to add
        """
        src_dir_r = await AsyncPath(src_dir).resolve()
        _log.debug(f"Resolving {src_dir} to {src_dir_r} for adding")
        new_dir = BackupSrc(path=str(src_dir_r))
        # Check if the directory is already in the database
        result = await self.aquery_one(BackupSrc, BackupSrc.path == str(src_dir_r))
        if result:
            raise FileExistsError(f"Directory {src_dir} already exists in the database")

        await self.add(new_dir)

    async def add_dest(self, dest_id: VolumeId) -> None:
        """Initialise a destination
        Ensure a unique entry exists for the destination id

        Args:
            dest_id (VolumeId): The destination ID
        """
        result = await self.aquery_one(BackupDest, BackupDest.name == dest_id)
        if not result:
            result = BackupDest(name=dest_id)
            await self.add(result)

    async def add_bkp_dest_to_backup_file(
        self, dest_name: VolumeId, backup_file: BackupFile
    ) -> None:
        new_tag = await self._query_dest_tag(dest_name)
        self.add_tag_obj_to_backup_file(new_tag, backup_file)

    async def add_bkp_dests_to_backup_file(
        self, dest_names: Sequence[VolumeId], backup_file: BackupFile
    ) -> None:
        dest_names = list(dest_names)
        gathered_tags = []
        for dest_name in dest_names:
            # FIXME Gather over the objects first, then bulk modify the backup file
            new_tag = await self._query_dest_tag(dest_name)
            gathered_tags.append(new_tag)
        for new_tag in gathered_tags:
            self.add_tag_obj_to_backup_file(new_tag, backup_file)

    @staticmethod
    def add_tag_obj_to_backup_file(
        new_tag: BackupDest | str, backup_file: BackupFile
    ) -> None:
        """Add a tag to the backed up file to say it is on the destination

        Args:
            new_tag (BackupDest|str): Tag for the file
            backup_file (BackupFile): The file being backed up
        """
        if new_tag.name not in backup_file.dest_names:
            backup_file.backup_dest.append(new_tag)
            backup_file.visited = 1

    async def _query_dest_tag(self, dest_id: VolumeId) -> BackupDest:
        result = await self.aquery_one(BackupDest, BackupDest.name == dest_id)
        return result or BackupDest(name=dest_id)

    async def _query_dest_tags(self, dest_ids: Iterable[VolumeId]) -> BackupDest:
        return await self.aquery_many(
            BackupDest,
            filter_options=[
                (BackupDest.name == dest_id, dest_id) for dest_id in dest_ids
            ],
            on_fail=lambda x: BackupDest(name=x),
        )

    async def for_backup(self, dest_name: VolumeId) -> Iterable[BackupFile]:
        # Query for files without a specified dest
        # Ordered by largest files first
        return await self.aquery_generator(
            BackupFile,
            ~BackupFile.backup_dest.any(BackupDest.name == dest_name),
            order_by=desc(BackupFile.size),
        )

    async def missing_files(self) -> AsyncGenerator[BackupFile, None]:
        file: BackupFile
        for file in await self.aquery_generator(BackupFile):
            file_path = AsyncPath(file.src_path) / file.filename
            if not await file_path.exists():
                yield file

    async def count_files_by_backup_dest_length(self) -> dict[int, int]:
        """Count the number of files grouped by the length of their backup_dest field.

        Returns:
            dict[int, int]: A dictionary where the key is the length of the backup_dest field,
                            and the value is the number of files with that length.
        """
        async with self._lock:
            result = await self.session.execute(select(BackupFile))
        lengths = (len(bkp_file.backup_dest) for bkp_file in result.unique().scalars())

        return dict(Counter(lengths))

    async def size_files_by_backup_dest_length(self) -> dict[int, int]:
        """Count the number of files grouped by the length of their backup_dest field.

        Returns:
            dict[int, int]: A dictionary where the key is the number of destinations backed up to,
                            and the value is the total size of files with that number of destinations.
        """
        async with self._lock:
            result = await self.session.execute(select(BackupFile))

        size_by_length = defaultdict(int)
        for bkp_file in bkp_file in result.unique().scalars():
            length = len(bkp_file.backup_dest)
            size_by_length[length] += bkp_file.size
        return size_by_length

    async def update_file(self, src_file: BkpFile, src_dir: AsyncPath) -> BackupFile:
        """Update the database with the specified BkpFile Object

        Args:
            src_file (BkpFile): The properties of the file in question

        Returns:
            BackupFile: The entry from the database
        """
        if src_file.file_path.is_absolute():
            file_path = src_file.file_path.relative_to(src_dir)
        else:
            file_path = src_file.file_path
        entry: BackupFile = await self.aquery_one(
            BackupFile, BackupFile.filename == str(file_path)
        )

        add_entry = entry is None
        fetched_tags: list[BackupDest] = await self._query_dest_tags(src_file.bkp_dests)
        if add_entry:
            entry = BackupFile(filename=str(file_path), src_path=str(src_dir))
            entry.backup_dest = fetched_tags
        else:
            entry = await self.aquery_one(
                BackupFile, BackupFile.filename == str(file_path)
            )
            async with self._lock:
                entry.backup_dest.clear()
                entry.backup_dest = fetched_tags
        if not (src_file.md5 and src_file.mtime):
            raise ValueError(
                f"MD5 and mtime must be set in the BkpFile object:{src_file}"
            )
        entry.size = src_file.size
        entry.md5_hash = src_file.md5
        entry.visited = 0
        entry.timestamp = datetime.fromtimestamp(src_file.mtime)
        entry.src_path = str(src_dir)
        if add_entry:
            async with self._lock:
                self._session_add(entry)
        return entry

    @staticmethod
    def delete_unvisited_files(file: BackupFile) -> bool:
        # Return true if we want the file deleted
        visited = file.visited
        file.visited = 0
        return not visited

    async def clear_all_visited(self):
        for file in await self.aquery_generator(BackupFile, BackupFile.visited != 0):
            file.visited = 0

    async def discovery(self, files: list[BkpFile], dest: VolumeId):
        """Discovery is the process of discovering backed up files already on a dest

        Args:
            files (list[BkpFile]): The files you have found on the dest
            dest (VolumeId): The VolumeId that you found them on
        """
        tag_obj = await self._query_dest_tag(dest)
        tasks = []
        for file in files:
            task = self._discover_one_file(file, tag_obj)
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def _discover_one_file(self, file: BkpFile, tag_obj: BackupDest):
        for matching_file in await self.aquery_generator(
            BackupFile, BackupFile.md5_hash == file.md5
        ):
            self.add_tag_obj_to_backup_file(tag_obj, matching_file)

    async def add_restore_context(self, restore_context: RestoreContext):
        for src_path, _ in restore_context.file_structure.items():
            # Check for existing entries in the database
            for _ in await self.aquery_generator(
                BackupFile, BackupFile.src_path == src_path
            ):
                raise ValueError(
                    f"Database already contains entries for {src_path}. Cannot add RestoreContext."
                )

        # Iterate through the RestoreContext
        for src_path, restore_directory in restore_context.file_structure.items():
            await self._add_directory_to_db(src_path, restore_directory)

    async def _add_directory_to_db(
        self, src_path: str, restore_directory: RestoreDirectory, parent_path: str = ""
    ):
        # Add files in the current directory
        # FIXME use a gather here
        for restore_file in restore_directory.files:
            backup_file = BackupFile(
                src_path=src_path,
                filename=str(Path(parent_path) / restore_file.name),
                size=restore_file.size,
                md5_hash=restore_file.md5,
                timestamp=datetime.fromtimestamp(restore_file.mtime or 0),
            )
            await self.add_bkp_dests_to_backup_file(restore_file.bkp_dests, backup_file)
            async with self._lock:
                self._session_add(backup_file)

        # Recursively add subdirectories
        for subdirectory in restore_directory.subdirectories:
            new_parent_path = str(Path(parent_path) / subdirectory.name)
            await self._add_directory_to_db(src_path, subdirectory, new_parent_path)

    async def populate(
        self,
        remote_files: list[BkpFile],
        callback: Callable[[AsyncPath, AsyncPath], Awaitable[None]],
    ):
        """Populate the file structs as described in db
        From a remote files location (i.e. a target)

        Args:
            remote_files (list[BkpFile]): The files discovered on the target.
            callback (callable): The async callback function to call with local and remote paths.
        """
        for remote_file in remote_files:
            for local_file in await self.aquery_generator(
                BackupFile,
                and_(
                    BackupFile.md5_hash == remote_file.md5,
                    BackupFile.size == remote_file.size,
                ),
            ):
                local_path = AsyncPath(local_file.src_path) / local_file.filename
                if not await local_path.is_file():
                    await callback(
                        local_path, AsyncPath(remote_file.file_path) / remote_file.name
                    )
