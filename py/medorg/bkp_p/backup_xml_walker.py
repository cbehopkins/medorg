import asyncio
import logging
import os
import stat
from typing import Awaitable, Callable, Optional

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

    async def _walk_directory(self, callback: DirWalker):
        async def walk(current_path: AsyncPath):

            # Asynchronously list all files and subdirectories using aiopath's glob
            entries = current_path.glob(
                "*"
            )  # We start the glob going, but don't use it yet
            bkp_xml = await self._create_xml(current_path)

            # Create a list to hold tasks for processing files
            file_processing_tasks = []
            file_names = set()
            file_entries = []
            dir_entries = []

            entry: AsyncPath
            async for entry in entries:
                if entry.name == XML_NAME:
                    continue
                stat_result_i: os.stat_result = await entry.stat()

                if stat.S_ISREG(stat_result_i.st_mode):
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

            # Now make sure that bkp_xml object is up to date before we...
            await asyncio.gather(*file_processing_tasks)

            file_processing_tasks.clear()
            bkp_xml._remove_if_not_in_set(file_names)
            if callback:
                for entry, stat_result_i in file_entries:
                    task = callback(current_path, entry, stat_result_i, bkp_xml)
                    if task:
                        file_processing_tasks.append(task)

            for entry, _ in dir_entries:
                file_processing_tasks.append(walk(entry))

            # Wait for all file processing tasks to complete
            await asyncio.gather(*file_processing_tasks)
            await bkp_xml.commit()

        # Start the directory walk
        walk_path = await self.root.resolve(strict=True)
        _log.debug(f"Starting directory walk at: {walk_path}")
        await walk(walk_path)
