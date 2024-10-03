from unittest.mock import AsyncMock
import pytest
from pathlib import Path
from medorg.common.bkp_file import BkpFile
from medorg.common.types import BackupFile
from medorg.database.bdsa import Bdsa
from medorg.database.database_handler import DatabaseHandler
from medorg.restore.structs import RestoreContext, RestoreDirectory, RestoreFile
from tests.database_helpers import query_all_files
from aiopath import AsyncPath

@pytest.fixture
def restore_context() -> RestoreContext:
    restore_context = RestoreContext(bdsa=None)
    restore_directory1 = RestoreDirectory("src1")
    restore_directory2 = RestoreDirectory("src2")

    file1 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=None,
        mtime=1234567890,
        md5="md5_1",
        bkp_dests={"dest1"},
    )
    file2 = RestoreFile(
        name="file2.txt",
        size=200,
        file_path=None,
        mtime=1234567891,
        md5="md5_2",
        bkp_dests={"dest2"},
    )
    file3 = RestoreFile(
        name="file3.txt",
        size=300,
        file_path=None,
        mtime=1234567892,
        md5="md5_3",
        bkp_dests={"dest3"},
    )

    restore_directory1.add_file(file1)
    restore_directory1.add_file(file2)
    restore_directory2.add_file(file3)

    restore_context.file_structure["src1"] = restore_directory1
    restore_context.file_structure["src2"] = restore_directory2

    return restore_context


async def verify_result(db_session: Bdsa, restore_context: RestoreContext):
    files = await query_all_files(db_session)
    assert len(files) == sum(
        len(directory.files) for directory in restore_context.file_structure.values()
    )

    for src_path, directory in restore_context.file_structure.items():
        for restore_file in directory.files:
            file: BackupFile = next(
                file for file in files if file.md5_hash == restore_file.md5
            )
            assert file.filename == restore_file.name
            assert file.size == restore_file.size
            assert file.src_path == src_path
            assert file.dest_names == set(restore_file.bkp_dests)


@pytest.mark.asyncio
async def test_add_restore_context(tmp_path: Path, restore_context: RestoreContext):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()

    async with db_handler.session_scope() as db_session:
        # Add the restore context to the database
        await db_session.add_restore_context(restore_context)

        # Verify the result
        await verify_result(db_session, restore_context)


@pytest.mark.asyncio
async def test_populate_files_from_restore_context(
    tmp_path: Path, restore_context: RestoreContext
):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    # Create a list of remote files
    remote_files = [
        BkpFile(md5="md5_1", size=100, file_path="remote_path1", name="file1.txt"),
        BkpFile(md5="md5_2", size=200, file_path="remote_path2", name="file2.txt"),
        BkpFile(md5="md5_3", size=300, file_path="remote_path3", name="file3.txt"),
    ]

    # Mock the callback function
    callback = AsyncMock()

    async with db_handler.session_scope() as db_session:
        # Add the restore context to the database
        await db_session.add_restore_context(restore_context)

        # Call compare_and_callback
        await db_session.populate(remote_files, callback)

        # Verify the callback was called for each file
        for remote_file in remote_files:
            local_file = next(
                file
                for file in await query_all_files(db_session)
                if file.md5_hash == remote_file.md5
            )
            local_path = AsyncPath(local_file.src_path) / local_file.filename
            remote_path = AsyncPath(remote_file.file_path) / remote_file.name
            callback.assert_any_call(local_path, remote_path)
