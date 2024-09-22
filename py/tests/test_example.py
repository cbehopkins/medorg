import asyncio
import pytest
from example import DatabaseHandler, DatabaseSession, File, Tag

pytest_plugins = ("pytest_asyncio",)


async def add_sample_data(session: DatabaseSession):
    # Add sample data to the session
    file1 = File(name="file1")
    file2 = File(name="file2")
    file3 = File(name="file3")

    tag1 = Tag(name="tag1")
    tag2 = Tag(name="tag2")

    file1.tags = [tag1, tag2]
    file2.tags = [tag1]
    file3.tags = [tag2]

    await session.add_all([file1, file2, file3])


async def make_session(tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    db_handler = DatabaseHandler(db_url)
    await db_handler.create_tables()
    async with db_handler.session_scope() as db_session:
        await add_sample_data(db_session)
    return db_handler


@pytest.mark.asyncio
async def test_query_file_one(tmp_path):
    db_handler = await make_session(tmp_path)
    async with db_handler.session_scope() as db_session:
        files = await db_session.query_filename("file1")
        assert len(files) == 1
        assert files[0].name == "file1"

        await db_session.add_new_file_with_tag("file4", "some_tag")
        files = await db_session.query_filename("file4")
        assert len(files) == 1
        assert files[0].name == "file4"


@pytest.mark.asyncio
async def test_query_duplicate_file_names(tmp_path):
    db_handler = await make_session(tmp_path)
    async with db_handler.session_scope() as db_session:
        files = await db_session.query_filename("file1")
        assert len(files) == 1
        assert files[0].name == "file1"
        with pytest.raises(ValueError):
            await db_session.add_new_file_with_tag("file1", "some_tag")


@pytest.mark.asyncio
async def test_query_files_without_tag(tmp_path):
    db_handler = await make_session(tmp_path)

    tag_to_exclude = "tag1"
    async with db_handler.session_scope() as db_session:
        result = await db_session.query_files_without_tag(tag_to_exclude)
        assert isinstance(result, list)
        assert len(result) == 1
        assert all(tag_to_exclude not in file.tags for file in result)


@pytest.mark.asyncio
async def test_query_files_with_tag(tmp_path):
    db_handler = await make_session(tmp_path)

    tag_to_include = "tag1"
    async with db_handler.session_scope() as db_session:
        result = [file async for file in db_session.aquery_files_by_tag(tag_to_include)]
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(tag_to_include not in file.tags for file in result)


@pytest.mark.asyncio
async def test_query_persistence(tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    db_handler_0 = DatabaseHandler(db_url)
    await db_handler_0.create_tables()
    async with db_handler_0.session_scope() as db_session:
        await add_sample_data(db_session)

    tag_to_include = "tag1"
    async with db_handler_0.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(tag_to_include not in file.tags for file in result)

    db_handler_1 = DatabaseHandler(db_url)
    async with db_handler_1.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(tag_to_include not in file.tags for file in result)


@pytest.mark.asyncio
async def test_clear_table(tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    db_handler_0 = DatabaseHandler(db_url)
    await db_handler_0.create_tables()
    async with db_handler_0.session_scope() as db_session:
        await add_sample_data(db_session)

    tag_to_include = "tag1"
    async with db_handler_0.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 2

    db_handler_1 = DatabaseHandler(db_url)
    async with db_handler_1.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 2

    db_handler_2 = DatabaseHandler(db_url)
    await db_handler_2.clear_files_table()
    async with db_handler_2.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 0
        existing_tag = await db_session.query_tag_by_name(tag_to_include)
        assert existing_tag, "The tag should still exist"

        # Can we add back in files
        await db_session.add_new_file_with_tag("bob's file", tag_to_include)
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 1

    db_handler_3 = DatabaseHandler(db_url)
    async with db_handler_3.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 1


@pytest.mark.asyncio
async def test_clear_table_simple(tmp_path):
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    db_handler_0 = DatabaseHandler(db_url)
    await db_handler_0.create_tables()
    async with db_handler_0.session_scope() as db_session:
        await add_sample_data(db_session)

    tag_to_include = "tag1"
    db_handler_2 = DatabaseHandler(db_url)
    await db_handler_2.clear_files_table()
    async with db_handler_2.session_scope() as db_session:
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 0

        # Can we add back in files
        await db_session.add_new_file_with_tag("bob's file", tag_to_include)
        result = await db_session.query_files_by_tag(tag_to_include)
        assert len(result) == 1
