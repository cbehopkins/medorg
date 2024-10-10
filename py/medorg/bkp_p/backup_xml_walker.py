import asyncio
import logging
import os
import stat
from typing import AsyncIterable, Awaitable, Callable, Iterator, Optional

from aiopath import AsyncPath

from medorg.bkp_p import XML_NAME

from .async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager

_log = logging.getLogger(__name__)


async def process_file(file_path, directory_data):
    # Replace this with your custom processing logic for each file
    print(f"Processing file: {file_path}, Directory Data: {directory_data}")
    await asyncio.sleep(1)  # Simulate some asynchronous processing


# Example directory callback for preprocessing
async def directory_callback(current_path, directory_data):
    print(f"Preprocessing directory: {current_path}")
    # Replace this with your custom preprocessing logic
    return f"Data for {current_path}"


DirWalker = Callable[
    [AsyncPath, AsyncPath, os.stat_result, AsyncBkpXml], Optional[Awaitable]
]
entryStat = tuple[AsyncPath, os.stat_result]
entryStats = list[entryStat]
class BackupXmlWalker(AsyncBkpXmlManager):
    def __init__(self, root: os.PathLike):
        self.root = AsyncPath(root)

    async def go_walk(self, walker: DirWalker):
        await self._walk_directory(walker)

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

            # Asynchronously list all files and subdirectories using aiopath's glob
            entries = current_path.glob(
                "*"
            )  # We start the glob going, but don't use it yet
            bkp_xml = await self._create_xml(current_path)
            file_entries, dir_entries = await self._visit_files_and_dirs(
                entries=entries, bkp_xml=bkp_xml
            )

            tasks = []
            if callback:
                for entry, stat_result_i in file_entries:
                    task = callback(current_path, entry, stat_result_i, bkp_xml)
                    if task:
                        tasks.append(task)

            for entry, _ in dir_entries:
                tasks.append(walk(entry))

            # Wait for all file processing tasks to complete
            await asyncio.gather(*tasks)
            await bkp_xml.commit()

        # Start the directory walk
        walk_path = await self.root.resolve(strict=True)
        _log.debug(f"Starting directory walk at: {walk_path}")
        await walk(walk_path)
