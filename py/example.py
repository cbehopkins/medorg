import asyncio
from typing import AsyncGenerator, Generator
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Table,
    delete,
    select,
)
from sqlalchemy.orm import (
    declarative_base,
    relationship,
)
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

Base = declarative_base()

file_tag_association = Table(
    "file_tag_association",
    Base.metadata,
    Column("file_id", Integer, ForeignKey("files.id")),
    Column("tag_id", Integer, ForeignKey("tags.id")),
)

class File(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    size = Column(Integer, default=0)
    tags = relationship(
        "Tag", secondary=file_tag_association, back_populates="files", lazy="selectin"
    )

class Tag(Base):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    files = relationship(
        "File", secondary=file_tag_association, back_populates="tags", lazy="selectin"
    )


class DatabaseSession:
    def __init__(self, session):
        self.session = session

    async def add_all(self, items):
        self.session.add_all(items)
        await self.session.commit()

    async def query_files_without_tag(self, tag_to_exclude):
        # Query for files without a specified tag
        result = (
            (
                await self.session.execute(
                    select(File).filter(~File.tags.any(Tag.name == tag_to_exclude))
                )
            )
            .scalars()
            .all()
        )
        return result

    async def query_files_by_tag(self, tag_name):
        query = select(File).filter(File.tags.any(Tag.name == tag_name))
        result = await self.session.execute(query)
        files = result.scalars().all()
        return files

    async def aquery_files_by_tag(self, tag_name) -> AsyncGenerator[File, None]:
        query = select(File).filter(File.tags.any(Tag.name == tag_name))
        result = await self.session.execute(query)
        files = result.scalars().all()

        # Use asyncio.gather to collect the results
        async_files = await asyncio.gather(
            *(self._process_file_async(file) for file in files)
        )

        # Yield each processed file
        for file in async_files:
            yield file

    async def _process_file_async(self, file: File) -> File:
        # Add any asynchronous processing logic here if needed
        # For now, just return the file as is
        return file

    async def query_tag_by_name(self, tag_name) -> Tag:
        results = (
            (await self.session.execute(select(Tag).filter(Tag.name == tag_name)))
            .scalars()
            .all()
        )
        if not results:
            return None
        assert len(results) == 1
        return results[0]

    async def query_files_by_size(self) -> AsyncGenerator[list[File], None]:
        query = select(File).order_by(File.size)
        result = await self.session.execute(query)

        current_size = None
        current_group = []

        async for file in result.scalars():
            if file.size != current_size:
                if current_group:
                    yield current_group
                current_group = [file]
                current_size = file.size
            else:
                current_group.append(file)

        if current_group:
            yield current_group

    async def add_new_file_with_tag(self, file_name, tag_name) -> None:
        # Query for an existing file with the same name
        existing_files = await self.query_filename(file_name)

        if existing_files:
            # If a file with the same name exists, raise an exception or handle accordingly
            raise ValueError(f"A file with the name '{file_name}' already exists.")

        # Query for the Tag instance with the given name
        existing_tag = await self.query_tag_by_name(tag_name)

        if existing_tag is None:
            # If the Tag doesn't exist, create a new one
            new_tag = Tag(name=tag_name)
            self.session.add(new_tag)
            await self.session.commit()
            existing_tag = new_tag

        # Add a new file with the specified tag
        new_file = File(name=file_name)
        new_file.tags.append(existing_tag)
        self.session.add(new_file)
        await self.session.commit()

    async def query_filename(self, name) -> File:
        result = (
            (await self.session.execute(select(File).filter(File.name == name)))
            .scalars()
            .all()
        )
        return result

    async def update_file_tags(self, file_id, tag_names):
        """
        Update the tags associated with a File.

        Args:
            file_id (int): The ID of the File.
            tag_names (list[str]): The names of the tags to associate with the File.

        Returns:
            None
        """
        # Get the File by ID
        file_to_update = await self.session.get(File, file_id)

        if file_to_update:
            # Clear existing tags
            file_to_update.tags.clear()

            # Query for existing tags by name
            existing_tags = [
                await self.query_tag_by_name(tag_name) for tag_name in tag_names
            ]

            # Add existing tags to the file
            file_to_update.tags.extend(existing_tags)

            # Commit the changes
            await self.session.commit()


class DatabaseHandler:
    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = create_async_engine(db_url, echo=True)

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session_scope(self):
        async with AsyncSession(self.engine) as session:
            yield DatabaseSession(session)
            await session.commit()

    async def clear_files_table(self):
        async with self.engine.begin() as conn:
            await conn.execute(delete(File))
