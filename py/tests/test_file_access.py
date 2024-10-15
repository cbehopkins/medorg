import asyncio
import itertools
import os
import random
import shutil
import string
from dataclasses import dataclass
from pathlib import Path

import pytest
from aiopath import AsyncPath
from lxml import etree

from medorg.bkp_p.async_bkp_xml import AsyncBkpXml
from medorg.bkp_p.backup_xml_walker import BackupXmlWalker
from medorg.cli.runners import (backup_files, create_update_db_file_entries,
                                remove_unvisited_files_from_database,
                                update_source_directory_entries,
                                writeback_db_file_entries)
from medorg.common import XML_NAME
from medorg.common.async_walker import walk
from medorg.common.bkp_file import BkpFile, calculate_md5
from medorg.database.database_handler import DatabaseHandler
from tests.database_helpers import aquery_all_files, query_all_files


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


SizeSource = itertools.count(step=10)
TimeSource = itertools.count(step=11)


@dataclass
class TstFile:
    filepath: str
    size: int = None
    md5: str = None
    timestamp: int = None

    def __post_init__(self):
        if self.size is None:
            self.size = next(SizeSource)
        if self.timestamp is None:
            self.timestamp = next(TimeSource)
        if self.md5 is None:
            self.md5 = id_generator()


@pytest.fixture
def dummy_files0():
    return [
        TstFile("bob.txt"),
        TstFile("fred.txt"),
    ]


def make_dummy_files(dir_path: AsyncPath, files: list[TstFile]):
    for file in files:
        tmp_path = dir_path / file.filepath
        tmp_path.touch()


@pytest.mark.asyncio
async def test_my_fakefs_async_pathlib(tmp_path, dummy_files0):
    make_dummy_files(tmp_path, dummy_files0)
    pth = AsyncPath(tmp_path)
    for file in dummy_files0:
        assert await (pth / file.filepath).is_file()
        assert not await (pth / file.filepath).is_dir()

    found_files = []
    async for top, dirs, files in walk(tmp_path):
        found_files.extend(files)
    assert len(found_files) == 2


def generate_random_data(size):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(size)
    )


def generate_file(directory, filename, size):
    filepath = os.path.join(directory, filename)
    data = generate_random_data(size)
    with open(filepath, "w") as file:
        file.write(data)


def generate_directory_structure(root_directory: str, structure):
    for name, item in structure:
        if isinstance(item, list):  # Handle subdirectory
            subdirectory_path = os.path.join(root_directory, name)
            os.makedirs(subdirectory_path)
            generate_directory_structure(subdirectory_path, item)
        else:  # Handle file
            generate_file(root_directory, name, item)


# Define the fixed structure
directory_structure = [
    ("file1.txt", 100),
    ("file2.txt", 150),
    (
        "subdir1",
        [
            ("file3.txt", 80),
            ("file4.txt", 120),
        ],
    ),
    (
        "subdir2",
        [
            ("file5.txt", 200),
            ("file6.txt", 90),
            (
                "subdir3",
                [
                    ("file7.txt", 180),
                ],
            ),
        ],
    ),
]


def expected_paths(root_directory: Path, structure):
    for name, item in structure:
        if isinstance(item, list):  # Handle subdirectory
            subdirectory_path = root_directory / name
            yield from expected_paths(subdirectory_path, item)
        else:  # Handle file
            yield root_directory / name


@pytest.mark.asyncio
async def test_walk_xml_creation(tmp_path: Path):
    tmp_tmp = tmp_path / "src"
    tmp_tmp.mkdir()
    generate_directory_structure(str(tmp_tmp), directory_structure)
    walker = BackupXmlWalker(tmp_tmp)
    file_count = 0

    def my_walker(
        dir_: AsyncPath,
        entry: AsyncPath,
        stat_result_i: os.stat_result,
        bkp_xml: AsyncBkpXml,
    ):
        nonlocal file_count
        file_count += 1
        if entry.name == "file1.txt":
            assert int(stat_result_i.st_size) == 100
            assert dir_ == entry.parent
            bob = bkp_xml[entry.name]
            assert bob.md5
            assert bob.mtime > 0
            assert isinstance(bob.mtime, int)
            assert bob.size == 100
            # Check that the sync and async calculate methods get the same result.
            calculated_checksum = calculate_md5(entry)
            assert calculated_checksum.decode("utf8") == bob.md5
        if entry.name == "file7.txt":
            assert int(stat_result_i.st_size) == 180
            assert dir_ == entry.parent
            assert dir_.name == "subdir3"
            assert dir_.parent.name == "subdir2"
            bob = bkp_xml[entry.name]
            assert bob.md5
            assert bob.mtime > 0
            assert isinstance(bob.mtime, int)
            assert bob.size == 180

    await walker.go_walk(walker=my_walker)
    assert file_count == 7


@pytest.mark.asyncio
async def test_walk_with_db(tmp_path):
    """Create or Update the DB file entries

    We will visit all the files
    Then delete any entries that we have not visited

    Args:
        session (Bdsa): The database to update
        src_dir (AsyncPath): Source Directory for the visiting
    """

    """Make sure we ca use the walk runner to walk the dummy file structure"""
    tmp_tmp = tmp_path / "src"
    tmp_tmp.mkdir()
    generate_directory_structure(tmp_tmp, directory_structure)
    db_handler = DatabaseHandler(tmp_path / "db")
    await db_handler.create_session()
    lock = asyncio.Lock()

    async def my_walker(
        dir_: AsyncPath,
        entry: AsyncPath,
        stat_result_i: os.stat_result,
        bkp_xml: AsyncBkpXml,
    ):
        bkp_file = bkp_xml[entry.name]
        async with lock:
            # FIXME the session itself should probably have a lock
            entry = await db_session.update_file(bkp_file, src_dir=AsyncPath(tmp_path))
        entry.visited = 1

    file_count = 0
    # Given our Xml Walker, with some temporary files
    walker = BackupXmlWalker(tmp_tmp)

    async with db_handler.session_scope() as db_session:
        # When we walk the directory
        await walker.go_walk(walker=my_walker)
        for entry in await aquery_all_files(db_session):
            # Then all files are marked as visited
            assert entry.visited == 1
            file_count += 1
    # And we have visited all the files
    assert file_count == 7


@pytest.mark.asyncio
async def test_walk_runner(tmp_path):
    """Make sure we can use the walk runner to walk the dummy file structure"""
    tmp_tmp = tmp_path / "src"
    tmp_tmp.mkdir()

    # Given our dummy file setup
    generate_directory_structure(tmp_tmp, directory_structure)
    db_handler = DatabaseHandler(tmp_path / "db")
    await db_handler.create_session()

    my_dest = "some destination"
    async with db_handler.session_scope() as db_session:
        # When we do a simple creation of a blank db structure
        await create_update_db_file_entries(db_session, tmp_tmp)

        # And we ask for files that will need to be backed up
        files = list(await db_session.for_backup(my_dest))

        # Then all files will need to be backed up
        assert len(files) == 7
        filenames = {Path(f.filename).name for f in files}
        for i in range(7):
            assert f"file{i+1}.txt" in filenames
        prev_size = None
        for entry in files:
            assert entry.size > 0
            if prev_size is not None:
                # Each file should get gradually smaller
                # Because we want to back up the largest files first
                assert entry.size <= prev_size
            prev_size = entry.size


@pytest.mark.asyncio
async def test_update_source_directory_entries(tmp_path):
    """For each visited file, do we correctly update the dest_id"""
    tmp_tmp = tmp_path / "src"
    tmp_tmp.mkdir()

    # Given our dummy file setup
    generate_directory_structure(tmp_tmp, directory_structure)
    db_handler = DatabaseHandler(tmp_path / "db")
    await db_handler.create_session()

    my_dest = "some destination"
    async with db_handler.session_scope() as db_session:
        # When we run a backup
        await db_session.add_src_dir(tmp_tmp)
        await create_update_db_file_entries(db_session, tmp_tmp)
        await remove_unvisited_files_from_database(db_session)
        # And we mark the files as have been backed up:
        await update_source_directory_entries(
            bdsa=db_session,
            dest_id=my_dest,
        )

        # Then all those files are marked as backed up
        files = list(await db_session.for_backup(my_dest))
        assert len(files) == 7


from unittest import mock


def list_files(pth: os.PathLike) -> list[str]:
    return list(Path(pth).rglob("*.txt"))


@pytest.mark.asyncio
@mock.patch("medorg.cli.runners.async_copy_file")
async def test_full_backup(mock_copy, tmp_path):
    mock_copy.side_effect = shutil.copy2
    """For each visited file, do we correctly update the dest_id"""
    tmp_path = AsyncPath(tmp_path)
    tmp_src = tmp_path / "src"
    await tmp_src.mkdir()
    tmp_dst = tmp_path / "dst"

    # Given our dummy file setup
    generate_directory_structure(tmp_src, directory_structure)
    db_handler = DatabaseHandler(tmp_path / "db")
    await db_handler.create_session()

    my_dest = "some destination"
    async with db_handler.session_scope() as db_session:
        # When we run a backup
        await db_session.add_src_dir(tmp_src)
        await backup_files(
            session=db_session,
            dest_path=tmp_dst,
            dest_id=my_dest,
        )
    call_props = [obj[0] for obj in mock_copy.call_args_list]
    src_dirs = [pth[0].relative_to(tmp_path) for pth in call_props]
    dst_dirs = [pth[1].relative_to(tmp_path) for pth in call_props]
    # Then we have copied all the files to ther correct plaxw
    assert mock_copy.call_count == 7
    assert all(p.parts[0] == "src" for p in src_dirs)
    assert all(p.parts[0] == "dst" for p in dst_dirs)
    expected_stuff = list(expected_paths(Path(""), directory_structure))
    src_reduction = {str(p.relative_to("src")) for p in src_dirs}
    dst_reduction = {str(p.relative_to("dst/src")) for p in dst_dirs}

    assert all(str(es) in src_reduction for es in expected_stuff)
    assert all(str(es) in dst_reduction for es in expected_stuff)
    # We have mocked the copy file
    assert len(list_files(tmp_dst)) == 7


@pytest.mark.asyncio
@mock.patch("medorg.cli.runners.async_copy_file")
async def test_backup_filling_drive(mock_copy, tmp_path):
    """For each visited file, do we correctly update the dest_id"""
    tmp_path = AsyncPath(tmp_path)
    tmp_src = tmp_path / "src"
    await tmp_src.mkdir()
    tmp_dst = tmp_path / "dst"

    def dummy_copy(src, dest, *args, **kwargs):
        if src.name == "file5.txt":
            raise IOError(
                "Drive is full-ish, still some space, but I can't accept a file that big"
            )
        shutil.copy2(str(src), str(dest))

    mock_copy.side_effect = dummy_copy

    # Given our dummy file setup
    generate_directory_structure(tmp_src, directory_structure)
    db_handler = DatabaseHandler(tmp_path / "db")
    await db_handler.create_session()

    my_dest = "some destination"
    async with db_handler.session_scope() as db_session:
        await db_session.add_src_dir(tmp_src)
        # When we run a backup
        await backup_files(
            session=db_session,
            dest_path=tmp_dst,
            dest_id=my_dest,
        )
    call_props = [obj[0] for obj in mock_copy.call_args_list]
    src_dirs = [pth[0].relative_to(tmp_path) for pth in call_props]
    assert mock_copy.call_count == 7
    assert len(src_dirs) == 7
    assert len(list_files(tmp_dst)) == 6


@pytest.mark.asyncio
async def test_discovery(tmp_path):
    """We have an existing backup dest, can we back prop the dest_id"""
    tmp_src = tmp_path / "src"
    tmp_src.mkdir()
    tmp_src = AsyncPath(tmp_src)

    # Given our dummy file setup
    generate_directory_structure(tmp_src, directory_structure)
    db_handler = DatabaseHandler(tmp_path / "db")
    await db_handler.create_session()

    my_dest = "some first destination"
    async with db_handler.session_scope() as db_session:
        await db_session.add_src_dir(tmp_src)
        # Given a file setup that has been marked as backed up already
        await create_update_db_file_entries(db_session, tmp_src)
        # just mark every file as backed up to my_dest
        for entry in await query_all_files(db_session):
            entry.visited = 1
        await update_source_directory_entries(
            bdsa=db_session,
            dest_id=my_dest,
        )
    # Check - has the xml been updated to include the dest_id
    root = etree.parse(tmp_src / XML_NAME).getroot()
    file_elem = root.find(".//fr[@fname='file1.txt']")
    file_elem[0].text == my_dest

    # Hacky bit, just grab the md5s:
    md5_map = {}

    def my_walker(
        dir_: AsyncPath,
        entry: AsyncPath,
        stat_result_i: os.stat_result,
        bkp_xml: AsyncBkpXml,
    ):
        bkp_file = bkp_xml[entry.name]
        assert bkp_file.md5
        md5_map[entry.name] = bkp_file.md5

    walker = BackupXmlWalker(tmp_src)
    await walker.go_walk(walker=my_walker)

    assert len(md5_map) == 7
    file_list = [
        BkpFile(
            name="filea",
            md5=md5_map["file1.txt"],
        )
    ]
    my_second_dest = "some second destination"

    async with db_handler.session_scope() as db_session:
        await db_session.discovery(files=file_list, dest=my_second_dest)
        await writeback_db_file_entries(db_session)

    root = etree.parse(tmp_src / XML_NAME).getroot()
    file_elem = root.find(".//fr[@fname='file1.txt']")
    # Here we look for, has the bkp xml been updated
    # To say the file is now backed up on two places
    assert len(file_elem) == 2
    destinations = {fe.text for fe in file_elem}
    assert {my_dest, my_second_dest} == destinations


def test_backup_from_a_src_with_multiple_files_same_hash_works(): ...
