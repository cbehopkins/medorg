from pathlib import Path

import pytest

from medorg.common.bkp_file import BkpFile
from medorg.database.database_handler import DatabaseHandler
from medorg.restore.structs import (RestoreContext, RestoreDirectory,
                                    RestoreFile)
from tests.database_helpers import add_file


def test_restore_file_equality():
    file1 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=Path("/path/to/file1.txt"),
        mtime=1234567890,
        md5="abc123",
        bkp_dests={"dest1"},
    )
    file2 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=Path("/path/to/file1.txt"),
        mtime=1234567890,
        md5="abc123",
        bkp_dests={"dest1"},
    )
    assert file1 == file2


def test_restore_directory_equality():
    dir1 = RestoreDirectory("root")
    dir2 = RestoreDirectory("root")

    file1 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=Path("/path/to/file1.txt"),
        mtime=1234567890,
        md5="abc123",
        bkp_dests={"dest1"},
    )
    dir1.add_file(file1)
    dir2.add_file(file1)

    assert dir1 == dir2


def test_restore_directory_inequality():
    dir1 = RestoreDirectory("root")
    dir2 = RestoreDirectory("root")

    file1 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=Path("/path/to/file1.txt"),
        mtime=1234567890,
        md5="abc123",
        bkp_dests={"dest1"},
    )
    file2 = RestoreFile(
        name="file2.txt",
        size=200,
        file_path=Path("/path/to/file2.txt"),
        mtime=1234567891,
        md5="def456",
        bkp_dests={"dest2"},
    )

    dir1.add_file(file1)
    dir2.add_file(file2)

    assert dir1 != dir2


def test_restore_directory_with_equal_files():
    dir1 = RestoreDirectory("root")
    dir2 = RestoreDirectory("root")

    file1 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=Path("/path/to/file1.txt"),
        mtime=1234567890,
        md5="abc123",
        bkp_dests={"dest1"},
    )
    file2 = RestoreFile(
        name="file1.txt",
        size=100,
        file_path=Path("/path/to/file1.txt"),
        mtime=1234567890,
        md5="abc123",
        bkp_dests={"dest1"},
    )
    assert file1 == file2

    dir1.add_file(file1)
    dir2.add_file(file2)

    assert dir1 == dir2


@pytest.fixture
def reference_file_structure() -> list[tuple[str, BkpFile]]:
    return [
        (
            "src1",
            BkpFile(
                name="dir1/file1.txt",
                size=100,
                mtime=1234567890,
                md5="md5_1",
                bkp_dests=["dest1"],
            ),
        ),
        (
            "src1",
            BkpFile(
                name="dir1/dir2/file2.txt",
                size=200,
                mtime=1234567891,
                md5="md5_2",
                bkp_dests=["dest2"],
            ),
        ),
        (
            "src2",
            BkpFile(
                name="file3.txt",
                size=300,
                mtime=1234567892,
                md5="md5_3",
                bkp_dests=["dest3"],
            ),
        ),
    ]


@pytest.fixture
def reference_restore_context() -> dict[str, RestoreDirectory]:
    struct = {
        "src1": RestoreDirectory("src1"),
        "src2": RestoreDirectory("src2"),
    }

    dir1 = RestoreDirectory("dir1")
    dir2 = RestoreDirectory("dir2")

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

    dir2.add_file(file2)
    dir1.add_file(file1)
    dir1.add_subdirectory(dir2)
    struct["src1"].add_subdirectory(dir1)
    struct["src2"].add_file(file3)
    return struct


@pytest.mark.asyncio
async def test_build_file_structure(
    tmp_path: Path,
    reference_file_structure: list[tuple[str, BkpFile]],
    reference_restore_context: dict[str, RestoreDirectory],
):
    # Staring from a database that has a
    # few files, build the Restore context associated with that
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()

    async with db_handler.session_scope() as db_session:
        for source, file in reference_file_structure:
            await add_file(
                db_session,
                file_name=file.name,
                dest_names=file.bkp_dests,
                size=file.size,
                timestamp=file.mtime,
                md5_hash=file.md5,
                src_path=source,
            )

        # Create an instance of RestoreContext and build the file structure
        restore_context = RestoreContext()
        file_structure = await restore_context.build_file_structure(db_session)
    # And check it matches what we expect
    assert file_structure == reference_restore_context
