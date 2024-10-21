import asyncio
import logging
import os
import stat
from typing import AsyncIterable, Awaitable, Callable, Iterator, Optional

from aiopath import AsyncPath

from medorg.common import RESERVED_NAMES, XML_NAME

from .async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager

_log = logging.getLogger(__name__)


DirWalker = Callable[
    [AsyncPath, AsyncPath, os.stat_result, AsyncBkpXml], Optional[Awaitable]
]
entryStat = tuple[AsyncPath, os.stat_result]
entryStats = list[entryStat]


class BackupXmlWalker(AsyncBkpXmlManager):
    def __init__(self, root: os.PathLike):
        self.root_dir = AsyncPath(root)
        self._lock = asyncio.Lock()
        self._counter = 0

    async def _incr_cnt(self, val: int) -> None:
        async with self._lock:
            self._counter += val

    async def _decr_cnt(self, val: int) -> None:
        async with self._lock:
            self._counter -= val
        assert self._counter >= 0

    async def go_walk(
        self, walker: DirWalker, on_dir_close: Callable[[], None] | None = None
    ):
        await self._walk_directory(walker)
        if on_dir_close:
            await on_dir_close()

    @staticmethod
    async def _create_xml(directory: AsyncPath) -> AsyncBkpXml:
        tmp = AsyncBkpXml(directory)
        await tmp.init_structs()
        return tmp

    async def _build_entry_stats(
        self, entries: AsyncIterable[AsyncPath]
    ) -> tuple[Iterator[asyncio.Future[entryStat]], AsyncPath | None]:
        """Build a list that has the stats for each entry in the directory.
        Do this concurrently so that we have all the data we need ready to go.

        Args:
            entries (AsyncIterable[AsyncPath]): glob of the directoruy we're working in

        Returns:
            entryStats: The file entry and its stats
            AsyncPath|None: Path to the XML file if it exists
        """
        xml_file = None
        the_list = []

        async def append_task(entry):
            stat_result_i: os.stat_result = await entry.stat()
            return (entry, stat_result_i)

        async for entry in entries:
            if entry.name in RESERVED_NAMES:
                if entry.name == XML_NAME:
                    xml_file = entry
                continue
            the_list.append(append_task(entry))
        entry_stats = asyncio.as_completed(the_list)
        return entry_stats, xml_file

    async def _visit_files_and_dirs(
        self, entries: AsyncIterable[AsyncPath], bkp_xml: AsyncBkpXml
    ) -> tuple[entryStats, entryStats]:
        """Now we have all the information about what is in the directiry
        We will visit any files there. This will build the bkp_xml object for this directory
        Any subdirectories we find will be returned as a list

        Args:
            entries (AsyncIterable[AsyncPath]): glob of the directoruy we're working in
            bkp_xml (AsyncBkpXml): The bk_xml object for this directory

        Returns:
            file_entries (entryStats): Files we found
            dir_entries (entryStats): Directories we found
        """
        entry_stats, xml_file = await self._build_entry_stats(entries)

        file_count = 0
        file_names = set()
        file_entries = []
        file_processing_tasks = []
        dir_entries = []

        entry: AsyncPath
        for task_result in entry_stats:
            entry, stat_result_i = await task_result
            if stat.S_ISREG(stat_result_i.st_mode):
                file_count += 1
                file_names.add(entry.name)
                file_entries.append((entry, stat_result_i))

                # If it's a file, start a new task to process it concurrently
                task = bkp_xml.visit_file(entry, stat_result_i)
                file_processing_tasks.append(task)
            elif stat.S_ISDIR(stat_result_i.st_mode):
                if entry.name.startswith("."):
                    # Skip hidden directories
                    continue
                dir_entries.append((entry, stat_result_i))
        if file_count == 0 and xml_file:
            file_processing_tasks.append(xml_file.unlink())
        bkp_xml._remove_if_not_in_set(file_names)

        # Gather so that the bkp_xml is ready to go
        await asyncio.gather(*file_processing_tasks)
        return file_entries, dir_entries

    async def _walk_directory(self, callback: DirWalker):
        async def walk(current_path: AsyncPath):
            print(f"Visiting:{current_path}")
            # Asynchronously list all files and subdirectories using aiopath's glob
            entries = current_path.glob(
                "*"
            )  # We start the glob going, but don't use it yet
            bkp_xml = await self._create_xml(current_path)
            file_entries, dir_entries = await self._visit_files_and_dirs(
                entries=entries, bkp_xml=bkp_xml
            )
            file_names_list = [entry for entry, _ in file_entries]
            file_names_set = set(file_names_list)
            if len(file_names_list) != len(file_names_set):
                _log.warning(
                    f"Duplicate file names in {current_path}::{file_names_list}"
                )
            tasks = []
            if callback:
                for entry, stat_result_i in file_entries:
                    if entry not in file_names_set:
                        # Skip duplicates
                        continue
                    file_names_set.remove(entry)
                    task = callback(current_path, entry, stat_result_i, bkp_xml)
                    if task:
                        tasks.append(task)

            # Wait for all file processing tasks to complete
            await self._incr_cnt(len(tasks))
            await asyncio.gather(*tasks)
            await self._decr_cnt(len(tasks))

            await bkp_xml.commit()
            del bkp_xml
            tasks.clear()
            for entry, _ in dir_entries:
                # Not bothering to lock as it's not critical
                if self._counter > 200:
                    await walk(entry)
                else:
                    tasks.append(walk(entry))
            if not tasks:
                return
            # Wait for all file processing tasks to complete
            await self._incr_cnt(len(tasks))
            for task in asyncio.as_completed(tasks):
                await task
                await self._decr_cnt(1)

        # Start the directory walk
        walk_path = await self.root_dir.resolve(strict=True)
        _log.debug(f"Starting directory walk at: {walk_path}")
        await walk(walk_path)
