import contextlib
from pathlib import Path

import pytest
from aiopath import AsyncPath

from medorg.common.bkp_file import BkpFile
from medorg.common.types import BackupFile
from medorg.database.bdsa import Bdsa
from medorg.database.database_handler import DatabaseHandler
from tests.database_helpers import (
    add_file,
    aquery_all_files,
    aquery_all_src_dirs,
    query_all_files,
    query_dest,
    query_files_visited,
    query_files_without_dest,
    query_hash,
    query_src_dir,
    visit_files,
)

pytest_plugins = ("pytest_asyncio",)


@pytest.mark.asyncio
async def test_add_src_dir(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        src_dir = tmp_path / "src_dir"
        await db_session.add_src_dir(src_dir)
        result = await query_src_dir(db_session, src_dir)
        assert result.path == str(src_dir)


@pytest.mark.asyncio
async def test_add_src_dir_duplicate(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        src_dir = tmp_path / "src_dir"
        await db_session.add_src_dir(src_dir)
        with pytest.raises(FileExistsError):
            await db_session.add_src_dir(src_dir)


@pytest.mark.asyncio
async def test_add_src_dir_relative_path(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        src_dir = tmp_path / "src_dir"
        relative_src_dir = Path("src_dir")
        with contextlib.chdir(tmp_path):
            await db_session.add_src_dir(relative_src_dir)
        result = await query_src_dir(db_session, src_dir)
        assert result.path == str(src_dir.resolve())


@pytest.mark.asyncio
async def test_aquery_all_src_dirs(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        src_dirs = [tmp_path / f"src_dir_{i}" for i in range(3)]
        for src_dir in src_dirs:
            await db_session.add_src_dir(src_dir)

        result = await aquery_all_src_dirs(db_session)
        result_paths = [src.path for src in result]
        expected_paths = [str(src_dir) for src_dir in src_dirs]
        assert sorted(result_paths) == sorted(expected_paths)


@pytest.mark.asyncio
async def test_add_src_dir_across_sessions_0(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    src_dir = tmp_path / "src_dir"

    async with db_handler.session_scope() as db_session:
        await db_session.add_src_dir(src_dir)

    async with db_handler.session_scope() as db_session:
        result = await query_src_dir(db_session, src_dir)
        assert result.path == str(src_dir)


@pytest.mark.asyncio
async def test_add_src_dir_across_sessions_1(tmp_path):
    db_path = tmp_path / "db"
    src_path = tmp_path / "src"
    db_handler_0 = DatabaseHandler(db_path)
    await db_handler_0.create_session()

    async with db_handler_0.session_scope() as db_session:
        await db_session.add_src_dir(src_path)

    db_handler_1 = DatabaseHandler(db_path)
    await db_handler_1.create_session()

    async with db_handler_1.session_scope() as db_session:
        result = await query_src_dir(db_session, src_path)
        assert result.path == str(src_path)


@pytest.mark.asyncio
async def test_aquery_all_src_dirs_across_sessions(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    src_dirs = [tmp_path / f"src_dir_{i}" for i in range(3)]

    async with db_handler.session_scope() as db_session:
        for src_dir in src_dirs:
            await db_session.add_src_dir(src_dir)

    async with db_handler.session_scope() as db_session:
        result = await aquery_all_src_dirs(db_session)
        result_paths = [src.path for src in result]
        expected_paths = [str(src_dir) for src_dir in src_dirs]
        assert sorted(result_paths) == sorted(expected_paths)


@pytest.mark.asyncio
async def test_query_file_one(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        await add_file(db_session, "file1", "some_tag")
        file = await db_session.aquery_one(BackupFile, BackupFile.filename == "file1")
        assert file.filename == "file1"


@pytest.mark.asyncio
async def test_query_two_files(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        await add_file(db_session, "file1", "some_tag")
        # Add another file for the same volume
        await add_file(db_session, "file2", "some_tag")
        file = await db_session.aquery_one(BackupFile, BackupFile.filename == "file1")
        assert file.filename == "file1"
        file = await db_session.aquery_one(BackupFile, BackupFile.filename == "file2")
        assert file.filename == "file2"

        # Now can we query by dest
        files = await query_dest(db_session, "some_tag")
        assert len(files) == 2
        filenames = [f.filename for f in files]
        assert "file1" in filenames
        assert "file2" in filenames


@pytest.mark.asyncio
async def test_query_missing_dest(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    my_dest = "some string"
    async with db_handler.session_scope() as db_session:
        await db_session.add_dest(my_dest)
        await add_file(db_session, "file1", my_dest)
        await add_file(db_session, "file2", my_dest)
        await add_file(db_session, "file3")
        files = await query_dest(db_session, my_dest)
        assert len(files) == 2
        files = await query_files_without_dest(db_session, my_dest)
        assert len(files) == 1


@pytest.mark.asyncio
async def test_query_missing_multi_dest(tmp_path):
    my_dest = "some string"
    my_other_dest = "some other string"
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        await db_session.add_dest(my_dest)
        await add_file(db_session, "file1", my_dest)
        await add_file(db_session, "file2", my_dest)
        await add_file(db_session, "file3", my_other_dest)
        await add_file(db_session, "file4", [my_dest, my_other_dest])
        files = await query_dest(db_session, my_dest)
        assert len(files) == 3
        files = await query_dest(db_session, my_other_dest)
        assert len(files) == 2
        files = await query_files_without_dest(db_session, my_dest)
        assert len(files) == 1
        files = await query_files_without_dest(db_session, my_other_dest)
        assert len(files) == 2
        report = await db_session.count_files_by_backup_dest_length()
        assert report == {1: 3, 2: 1}


@pytest.mark.asyncio
async def test_query_persistence(tmp_path):
    db_handler_0 = DatabaseHandler(tmp_path)
    await db_handler_0.create_session()
    my_dest = "some string"
    async with db_handler_0.session_scope() as db_session:
        await db_session.add_dest(my_dest)
        await add_file(db_session, "file1", my_dest, md5_hash="123456")
        await add_file(db_session, "file2", my_dest, md5_hash="789a")

    async with db_handler_0.session_scope() as db_session:
        files = await query_dest(db_session, my_dest)
        assert len(files) == 2

    # Now creating another db that should read back in that file
    db_handler_1 = DatabaseHandler(tmp_path)

    await db_handler_1.create_session()
    async with db_handler_1.session_scope() as db_session:
        files = await query_dest(db_session, my_dest)
        assert len(files) == 2


@pytest.mark.asyncio
async def test_query_persistence_hash(tmp_path):
    db_handler_0 = DatabaseHandler(tmp_path)
    await db_handler_0.create_session()
    my_dest = "some string"
    # Given a backup session that has had files backed up
    # to a dest
    async with db_handler_0.session_scope() as db_session:
        await db_session.add_dest(my_dest)
        await add_file(db_session, "file1", my_dest, md5_hash="123456")
        await add_file(db_session, "file2", my_dest, md5_hash="789a")

    # When we query them by their hash
    async with db_handler_0.session_scope() as db_session:
        file_obj = await query_hash(db_session, "123456")
        assert len(file_obj) == 1
        # Then the dest_id is persisted
        assert file_obj[0].md5_hash == "123456"
        assert file_obj[0].backup_dest[0].name == my_dest


@pytest.mark.asyncio
async def test_query_ordering(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    my_dest = "some string"
    async with db_handler.session_scope() as db_session:
        await add_file(
            db_session,
            "file1",
            size=30,
        )
        await add_file(
            db_session,
            "file0",
            size=300,
        )
        await add_file(
            db_session,
            "file2",
            size=3,
        )
        await add_file(
            db_session,
            "file3",
            size=30,
            dest_names=[my_dest],
        )

        files = list(await db_session.for_backup(my_dest))
        assert len(files) == 3
        assert files[0].filename == "file0"
        assert files[1].filename == "file1"
        assert files[2].filename == "file2"


@pytest.mark.asyncio
async def test_query_table_clear(tmp_path):
    db_handler_0 = DatabaseHandler(tmp_path)
    await db_handler_0.create_session()
    my_dest = "some string"
    async with db_handler_0.session_scope() as db_session:
        await db_session.add_dest(my_dest)
        await add_file(db_session, "file1", my_dest)
        await add_file(db_session, "file2", my_dest)

    async with db_handler_0.session_scope() as db_session:
        files = await query_dest(db_session, my_dest)
        assert len(files) == 2

    # Now creating another db that should read back in that file
    # And clearing the file data as if we had started again
    db_handler_1 = DatabaseHandler(tmp_path)
    await db_handler_1.create_session()
    await db_handler_1.clear_files()
    async with db_handler_1.session_scope() as db_session:
        await db_session.session.commit()
        files = await query_dest(db_session, my_dest)
        assert len(files) == 0

        # But can we still create new ones?
        await add_file(db_session, "file3", my_dest)
        await add_file(db_session, "file4", my_dest)
        files = await query_dest(db_session, my_dest)
        filenames = [ent.filename for ent in files]
        assert len(files) == 2, f"too many files {filenames}"

    async with db_handler_1.session_scope() as db_session:
        files = await query_dest(db_session, my_dest)
        assert len(files) == 2


@pytest.mark.asyncio
async def test_dummy_bkp_steps(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    my_dest = "some string"
    # So let's pretend we've done the initial file walk and
    # found file1 that is backed up to my_dest
    dummy_file = Path("some/dummy/file_path.fls")
    file_props = BkpFile(
        name=dummy_file.name,
        file_path=dummy_file,
        size=30,
        mtime=20,
        md5="Some String",
        bkp_dests=[],
    )
    async with db_handler.session_scope() as db_session:
        await db_session.update_file(file_props, src_dir=AsyncPath(tmp_path))
    async with db_handler.session_scope() as db_session:
        files = await query_dest(db_session, my_dest)
        assert len(files) == 0
    # Add in the backup
    file_props.bkp_dests.append(my_dest)
    async with db_handler.session_scope() as db_session:
        entry = await db_session.update_file(file_props, src_dir=AsyncPath(tmp_path))
        entry.visited = 1

        files = await query_dest(db_session, my_dest)
        assert len(files) == 1
        # Check that we can go back and play with any visited files
        files = await query_files_visited(db_session)
        assert len(files) == 1
        for file in files:
            file.visited = 0
        files = await query_files_visited(db_session)
        assert len(files) == 0


@pytest.mark.asyncio
async def test_dummy_bkp_source_files_deleted(tmp_path):
    """Test that the DBs current file list does not contain files from previous backups
    if those files were deleted"""
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    # We a first backup run with some source files
    my_source_files_0 = {"file_0", "file_1", "file_2"}
    # a second backup run with different files
    my_source_files_1 = {"file_1", "file_2", "file_3", "file_4", "file_5"}
    # Note: file_0 missing
    missing_files = my_source_files_0 - my_source_files_1
    assert len(missing_files) == 1

    def bkp_file(filename: str) -> BkpFile:
        file_path = Path(filename)
        return BkpFile(
            name=file_path.name,
            file_path=file_path,
            size=0,
            mtime=1,
            md5="some string",
        )

    # Okay, pass 1!
    async with db_handler.session_scope() as db_session:
        props_src = [bkp_file(filename) for filename in my_source_files_0]
        await visit_files(db_session, props_src, src_dir=AsyncPath(tmp_path))

        files = await aquery_all_files(db_session)
        assert len(files) == len(my_source_files_0)

    # Now for pass 2
    async with db_handler.session_scope() as db_session:
        props_src = [bkp_file(filename) for filename in my_source_files_1]
        await visit_files(db_session, props_src, src_dir=AsyncPath(tmp_path))
        await db_session.filter(Bdsa.delete_unvisited_files, BackupFile)
        files = await query_all_files(db_session)
        assert len(files) == len(my_source_files_1)

    # The database does not contain the files that have dissapeared
    async with db_handler.session_scope() as db_session:
        files = await query_all_files(db_session)
        assert len(files) == len(my_source_files_1)
        found_filenames = {entry.filename for entry in files}
        assert not missing_files.intersection(found_filenames)
        assert found_filenames == my_source_files_1


@pytest.mark.asyncio
async def test_restore_process_basic(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()

    # So we have some files we back up across two seessions
    my_source_files_0 = ["file_0", "file_1", "file_2"]
    my_source_files_1 = ["file_3", "file_4", "file_5"]

    async with db_handler.session_scope() as db_session:
        for file in my_source_files_0:
            await add_file(db_session, file, dest_names=["dest 0"])
        for file in my_source_files_1:
            await add_file(db_session, file, dest_names=["dest 1"])

    # When we restore, we get all source files from both runs
    async with db_handler.session_scope() as db_session:
        files = await query_all_files(db_session)
        expected_filenames = set(my_source_files_0)
        expected_filenames.update(my_source_files_1)
        assert len(files) == len(expected_filenames)
        for entry in files:
            assert entry.filename in expected_filenames


@pytest.mark.asyncio
async def test_restore_process_multiple_dests(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    # A file exists on multiple dests
    # We are given the option to restore the backup from either
    # So we have some files we back up across two seessions
    my_source_files_0 = ["file_0", "file_1", "file_2"]
    my_source_files_1 = ["file_3", "file_4", "file_5"]
    my_source_files_2 = ["file_6", "file_7", "file_8"]

    async with db_handler.session_scope() as db_session:
        for file in my_source_files_0:
            await add_file(db_session, file, dest_names=["dest 0"])
        for file in my_source_files_1:
            await add_file(db_session, file, dest_names=["dest 0", "dest 1"])
        for file in my_source_files_2:
            await add_file(db_session, file, dest_names=["dest 1"])

    # When we restore, we get source files from correct dest
    async with db_handler.session_scope() as db_session:
        for entry in await aquery_all_files(db_session):
            if entry.filename in my_source_files_0:
                assert entry.dest_names == {"dest 0"}
            if entry.filename in my_source_files_1:
                assert entry.dest_names == {"dest 0", "dest 1"}
            if entry.filename in my_source_files_2:
                assert entry.dest_names == {"dest 1"}


@pytest.mark.asyncio
async def test_restore_process_dir_hierarchy_preserved(tmp_path):
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()

    # So we have some files we back up from two different paths
    my_source_files_0 = ["file_0", "file_1", "file_2"]
    my_source_files_1 = ["file_3", "file_4", "file_5"]

    async with db_handler.session_scope() as db_session:
        for file in my_source_files_0:
            await add_file(db_session, f"dira/{file}", dest_names=["dest 0"])
        for file in my_source_files_1:
            await add_file(db_session, f"dirb/{file}", dest_names=["dest 0"])

    # When we restore, get filepaths correct
    async with db_handler.session_scope() as db_session:
        for entry in await aquery_all_files(db_session):
            filepath = Path(entry.filename)

            if filepath.name in my_source_files_0:
                assert str(filepath.parent) == "dira"
            if filepath.name in my_source_files_1:
                assert str(filepath.parent) == "dirb"


@pytest.mark.asyncio
async def test_discover_process(tmp_path):
    """Test discovering files already existing on a dest are recognised"""
    db_handler = DatabaseHandler(tmp_path)
    await db_handler.create_session()
    my_source_files_0 = {
        "file_0": {
            "md5": "1234",
        },
        "file_1": {
            "md5": "5678",
        },
        "file_2": {
            "md5": "9abc",
        },
    }
    async with db_handler.session_scope() as db_session:
        for filename, props in my_source_files_0.items():
            await add_file(db_session, file_name=filename, md5_hash=props["md5"])

    async with db_handler.session_scope() as db_session:
        file_1234 = await query_hash(db_session, "1234")
        assert file_1234 is not None
        assert len(file_1234) == 1
        assert file_1234[0].filename == "file_0"

        file_missing = await query_hash(db_session, "0123")
        assert not file_missing

    dummy_dest = "here"
    async with db_handler.session_scope() as db_session:
        # We have a file with the same hash, but a different name
        file_list = [
            BkpFile(
                name="filea",
                md5="5678",
            )
        ]
        await db_session.discovery(
            files=file_list,
            dest=dummy_dest,
        )
        # When we query for files backed up to that dest
        files = await query_dest(db_session, dummy_dest)
        assert len(files) == 1
        # Then the file with the different filename but same hash is there
        assert files[0].filename == "file_1"

        # Pretend we run it again with the same discovery
        await db_session.discovery(
            files=file_list,
            dest=dummy_dest,
        )
        # We should get the same result
        files = await query_dest(db_session, dummy_dest)
        assert len(files) == 1
        assert files[0].filename == "file_1"


def test_files_on_dest_are_not_added_to_restore_requirements(): ...
