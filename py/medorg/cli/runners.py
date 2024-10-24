import asyncio
import logging
import os
from copy import copy
from pathlib import Path

from aiopath import AsyncPath

from medorg.bkp_p.async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager
from medorg.bkp_p.backup_xml_walker import BackupXmlWalker
from medorg.common.bkp_file import BkpFile
from medorg.common.file_utils import async_copy_file
from medorg.common.types import BackupFile, BackupSrc, VolumeId
from medorg.database.bdsa import Bdsa

_log = logging.getLogger(__name__)


def _to_async_path(src_dir: AsyncPath | str | BackupSrc) -> AsyncPath:
    if isinstance(src_dir, BackupSrc):
        return AsyncPath(src_dir.path)
    return AsyncPath(src_dir)


async def create_update_db_file_entries(
    session: Bdsa, src_dir: AsyncPath | str | BackupSrc
):
    """Create or Update the DB file entries

    We will visit all the files
    Then delete any entries that we have not visited

    Args:
        session (Bdsa): The database to update
        src_dir (AsyncPath): Source Directory for the visiting
    """
    src_dir_i = _to_async_path(src_dir)

    async def my_walker(
        dir_: AsyncPath,
        entry: AsyncPath,
        stat_result_i: os.stat_result,
        bkp_xml: AsyncBkpXml,
    ):
        bkp_file: BkpFile = bkp_xml[entry.name]
        backup_file: BackupFile = await session.update_file(bkp_file, src_dir=src_dir_i)
        if not backup_file.md5_hash:
            msg = f"Missing md5 hash {backup_file}"
            _log.error(msg)
            raise RuntimeError(msg)
        if not backup_file.timestamp:
            msg = f"Missing timestamp {backup_file}"
            _log.error(msg)
            raise RuntimeError(msg)
        backup_file.visited = 1

    walker = BackupXmlWalker(src_dir_i)
    await walker.go_walk(walker=my_walker, on_dir_close=session.commit)


async def remove_unvisited_files_from_database(session: Bdsa):
    # Delete from the database any files we have not visited
    # and set visited to false
    # FIXME add a parameter to match specific source directory
    await session.filter(Bdsa.delete_unvisited_files, BackupFile)


async def writeback_db_file_entries(session: Bdsa):
    """Ensure we have written back changes to the files

    We will visit all the files
    Then delete any entries that we have not visited

    Args:
        session (Bdsa): The database to update
        src_dir (AsyncPath): Source Directory for the visiting
    """
    async with AsyncBkpXmlManager() as bkp_xmls:

        async def update_file_entry(file_entry: BackupFile):
            dir_path = AsyncPath(file_entry.src_path)
            filename = file_entry.filename
            assert not AsyncPath(filename).is_absolute()
            bkp_xml_src: AsyncBkpXml = bkp_xmls[dir_path]
            assert bkp_xml_src

            # FIXME this is gronky - but is awaiting a getitem worse?
            await bkp_xml_src.init_structs()
            # FIXME is there not a backupfile from file entry method?
            file_bob: BkpFile = bkp_xml_src[filename]
            file_bob.bkp_dests = file_entry.dest_names
            file_bob.md5 = file_entry.md5_hash
            file_bob.size = file_entry.size
            file_bob.mtime = int(file_entry.timestamp.timestamp())
            bkp_xml_src[filename] = file_bob

        for entry in await session.aquery_generator(
            BackupFile, BackupFile.visited != 0
        ):
            await update_file_entry(entry)


async def backup_files(session: Bdsa, dest_path: AsyncPath, dest_id: VolumeId):
    # Create an entry in the database for each file entry
    for src in await session.aquery_generator(BackupSrc):
        await create_update_db_file_entries(session, src)
    await remove_unvisited_files_from_database(session)
    async with AsyncBkpXmlManager() as bkp_xmls:
        await copy_best_files(session, dest_path, dest_id, bkp_xmls)

    # After all files are backed up, update the source directory entries
    await update_source_directory_entries(session, dest_id)


async def generate_src_dest_full_paths(file_entry: BackupFile, dest_path: AsyncPath):
    src_file_path = AsyncPath(file_entry.filename)
    if not src_file_path.is_absolute():
        src_file_path = AsyncPath(file_entry.src_path) / src_file_path

    relative_path = src_file_path.relative_to(file_entry.src_path)

    dest_file_path = dest_path / Path(file_entry.src_path).name / relative_path
    if not await src_file_path.is_file():
        raise FileNotFoundError(
            f"File {src_file_path} not found in generate_src_dest_full_paths"
        )
    return src_file_path, dest_file_path


async def copy_best_files(
    session: Bdsa,
    dest_path: AsyncPath,
    dest_id: VolumeId,
    bkp_xmls: AsyncBkpXmlManager,
):
    tasks = []
    for file_entry in await session.for_backup(dest_id):

        src_full_path, dest_file_path = await generate_src_dest_full_paths(
            file_entry, dest_path
        )
        task = asyncio.create_task(
            _backup_file(dest_id, src_full_path, dest_file_path, file_entry, bkp_xmls)
        )
        tasks.append(task)
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        _log.error(f"Error backing up files: {e}")
    print(tasks)


async def _backup_file(
    dest_id,
    src_file_path: AsyncPath,
    dest_file_path: AsyncPath,
    file_entry: BackupFile,
    bkp_xmls: AsyncBkpXmlManager,
):
    if not await src_file_path.is_file():
        _log.error(f"File {src_file_path} not found in _backup_file")
        return
    assert (
        not file_entry.visited
    ), "When backing up the file, it should not have been visited already"
    try:
        await dest_file_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _log.error(f"Failed to create directory {dest_file_path.parent}: {e}")
        return
    bkp_xml_src = bkp_xmls[src_file_path.parent]
    await bkp_xml_src.init_structs()  # FIXME this is awful
    if bkp_xml_src.root is None:
        raise RuntimeError("Root is None")
    current_file_data_src = bkp_xml_src[src_file_path.name]
    if dest_id in current_file_data_src.bkp_dests:
        _log.info(f"Not copying {src_file_path}, as it already is at dest {dest_id}")
        return
    bkp_xml_dest: AsyncBkpXml = bkp_xmls[dest_file_path.parent]
    try:
        await bkp_xml_dest.init_structs()  # FIXME this is awful
    except Exception as e:
        _log.error(f"Failed to init structs for {dest_file_path.parent}: {e}")
        return
    if bkp_xml_dest.root is None:
        raise RuntimeError("Root is None")

    current_file_data_dest = copy(current_file_data_src)
    current_file_data_dest.file_path = dest_file_path
    current_file_data_dest.size = current_file_data_src.size
    current_file_data_dest.mtime = current_file_data_src.mtime
    current_file_data_dest.md5 = current_file_data_src.md5
    try:
        assert await src_file_path.is_file()
        assert await dest_file_path.parent.is_dir()
        await async_copy_file(src_file_path, dest_file_path)
    except IOError as e:
        # As long as we return without marking it as visited...
        return
    assert current_file_data_dest.mtime != "None"
    assert current_file_data_dest.size != "None"
    assert current_file_data_dest.mtime is not None
    assert current_file_data_dest.size is not None
    bkp_xml_dest[dest_file_path.name] = current_file_data_dest
    # Backup successful, update the visited flag
    file_entry.visited = 1


async def update_source_directory_entries(bdsa: Bdsa, dest_id: VolumeId):
    # Go back and update the source directory data
    # Telling it about where the files have been backed up to
    async with AsyncBkpXmlManager() as bkp_xmls:

        async def update_file_entry(file_entry: BackupFile):

            await bdsa.add_bkp_dest_to_backup_file(dest_id, file_entry)
            full_src_filepath = AsyncPath(file_entry.src_path) / file_entry.filename
            assert await full_src_filepath.is_file()
            dir_path = full_src_filepath.parent
            filename = full_src_filepath.name
            bkp_xml_src: AsyncBkpXml = bkp_xmls[dir_path]

            # FIXME this is gronky - but is awaiting a getitem worse?
            await bkp_xml_src.init_structs()
            await bkp_xml_src.visit_file(
                full_src_filepath, (await full_src_filepath.stat())
            )
            # Modify backup dest attribute in the xml file
            file_props: BkpFile = bkp_xml_src[filename]
            file_props.bkp_dests.add(dest_id)
            assert None not in [
                file_props.size,
                file_props.mtime,
                file_props.md5,
            ], f"File {file_props} is missing data"
            bkp_xml_src[filename] = file_props

        for entry in await bdsa.aquery_generator(BackupFile, BackupFile.visited != 0):
            await update_file_entry(entry)
