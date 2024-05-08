import asyncio
import logging
import os
import shutil
from copy import copy
from bkp_db.types import BackupFile
from bkp_p.async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager
from bkp_p.backup_xml_walker import BackupXmlWalker
from bkp_p.bdsa import Bdsa
from bkp_p.bkp_xml import BkpFile
from bkp_p.volume_id import VolumeId
from bkp_p.async_walker import walk
from aiopath import AsyncPath
from bkp_p.async_bkp_xml import AsyncBkpXml

_log = logging.getLogger(__name__)


async def copy_file(src: AsyncPath, dest: AsyncPath):
    # FIXME find an awaitable version of this
    shutil.copy2(str(src), str(dest))


async def create_update_db_file_entries(session: Bdsa, src_dir: AsyncPath):
    """Create or Update the DB file entries

    We will visit all the files
    Then delete any entries that we have not visited

    Args:
        session (Bdsa): The database to update
        src_dir (AsyncPath): Source Directory for the visiting
    """

    async def my_walker(
        dir_: AsyncPath,
        entry: AsyncPath,
        stat_result_i: os.stat_result,
        bkp_xml: AsyncBkpXml,
    ):
        bkp_file = bkp_xml[entry.name]
        entry = await session.update_file(bkp_file)
        entry.visited = 1

    walker = BackupXmlWalker(src_dir)
    await walker.go_walk(walker=my_walker)
    # Delete from the database any files we have not visited
    # and set visited to false
    await session.filter(Bdsa.delete_unvisited_files)


async def writeback_db_file_entries(session: Bdsa, src_dir: AsyncPath):
    """Ensure we have written back changes to the files

    We will visit all the files
    Then delete any entries that we have not visited

    Args:
        session (Bdsa): The database to update
        src_dir (AsyncPath): Source Directory for the visiting
    """
    async with AsyncBkpXmlManager() as bkp_xmls:

        async def update_file_entry(file_entry: BackupFile):
            full_src_filepath = src_dir / file_entry.filename
            assert await full_src_filepath.is_file()
            dir_path = full_src_filepath.parent
            filename = full_src_filepath.name
            bkp_xml_src: AsyncBkpXml = bkp_xmls[dir_path]

            # FIXME this is gronky - but is awaiting a getitem worse?
            await bkp_xml_src.init_structs()
            # FIXME is there not a backupfile from file entry method?
            file_bob: BkpFile = bkp_xml_src[filename]
            file_bob.bkp_dests = file_entry.dest_names
            file_bob.md5 = file_entry.md5_hash
            file_bob.size = file_entry.size
            bkp_xml_src[filename] = file_bob

        for entry in await session.query_files_visited():
            await update_file_entry(entry)


async def backup_files(
    session: Bdsa, src_dir: AsyncPath, dest_path: AsyncPath, dest_id: VolumeId
):
    # Create an entry in the database for each file entry
    try:
        await create_update_db_file_entries(session, src_dir)

    except Exception as e:
        print(f"Error populating files for backup: {e}")

    async with AsyncBkpXmlManager() as bkp_xmls:
        await copy_best_files(session, src_dir, dest_path, dest_id, bkp_xmls)

    try:
        # After all files are backed up, update the source directory entries
        await update_source_directory_entries(session, src_dir, dest_id)
    except Exception as e:
        print("Error committing back xml changes")


async def copy_best_files(
    session: Bdsa,
    src_dir: AsyncPath,
    dest_path: AsyncPath,
    dest_id: VolumeId,
    bkp_xmls,
):
    tasks = []
    for file_entry in await session.for_backup(dest_id):
        src_file_path = src_dir / file_entry.filename

        relative_path = AsyncPath(file_entry.filename).relative_to(src_dir)
        dest_file_path = dest_path / relative_path
        assert await src_file_path.is_file()
        task = asyncio.create_task(
            backup_file(dest_id, src_file_path, dest_file_path, file_entry, bkp_xmls)
        )
        tasks.append(task)

    await asyncio.gather(*tasks)


async def backup_file(
    dest_id,
    src_file_path: AsyncPath,
    dest_file_path: AsyncPath,
    file_entry: BackupFile,
    bkp_xmls: AsyncBkpXmlManager,
):
    assert (
        not file_entry.visited
    ), "When backing up the file, it should not have been visited already"
    await dest_file_path.parent.mkdir(parents=True, exist_ok=True)
    bkp_xml_src = bkp_xmls[src_file_path.parent]
    await bkp_xml_src.init_structs()  # FIXME this is awful
    current_file_data_src = bkp_xml_src[src_file_path.name]
    if dest_id in current_file_data_src.bkp_dests:
        _log.info(f"Not copying {src_file_path}, as it already is at dest {dest_id}")
        return
    bkp_xml_dest: AsyncBkpXml = bkp_xmls[dest_file_path.parent]
    await bkp_xml_dest.init_structs()  # FIXME this is awful

    current_file_data_dest = copy(current_file_data_src)
    current_file_data_dest.file_path = dest_file_path
    bkp_xml_dest[dest_file_path.name] = current_file_data_dest

    try:
        await copy_file(src_file_path, dest_file_path)
    except IOError as e:
        # As long as we return without marking it as visited...
        return

    # Backup successful, update the visited flag
    file_entry.visited = 1


async def update_source_directory_entries(
    session: Bdsa, src_dir: AsyncPath, dest_id: str
):
    # Go back and update the source directory data
    # Telling it about where the files have been backed up to
    async with AsyncBkpXmlManager() as bkp_xmls:

        async def update_file_entry(file_entry: BackupFile):
            await session.add_bkp_dests_to_backup_file([dest_id], file_entry)
            full_src_filepath = src_dir / file_entry.filename
            assert await full_src_filepath.is_file()
            dir_path = full_src_filepath.parent
            filename = full_src_filepath.name
            bkp_xml_src: AsyncBkpXml = bkp_xmls[dir_path]

            # FIXME this is gronky - but is awaiting a getitem worse?
            await bkp_xml_src.init_structs()
            # Modify backup dest attribute in the xml file
            file_props: BkpFile = bkp_xml_src[filename]
            file_props.bkp_dests.add(dest_id)
            bkp_xml_src[filename] = file_props

        for entry in await session.query_files_visited():
            await update_file_entry(entry)
