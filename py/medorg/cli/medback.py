import asyncio
import importlib.metadata
import os
from functools import wraps
from pathlib import Path
from typing import IO

import click
from aiopath import AsyncPath

from medorg.bkp_p.async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager
from medorg.bkp_p.backup_xml_walker import BackupXmlWalker
from medorg.cli.runners import (
    copy_best_files,
    create_update_db_file_entries,
    remove_unvisited_files_from_database,
    update_source_directory_entries,
    writeback_db_file_entries,
)
from medorg.common.bkp_file import BkpFile
from medorg.common.file_utils import async_copy_file
from medorg.common.types import BackupSrc
from medorg.database.database_handler import DatabaseHandler
from medorg.restore.structs import RestoreContext
from medorg.volume_id.volume_id import VolumeIdSrc


# Stolen from https://github.com/pallets/click/issues/85
def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


VERSION = importlib.metadata.version("medorg")


@click.group()
@click.version_option(VERSION)
def cli():
    pass


@cli.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database.",
)
@click.option(
    "--target-dir",
    required=True,
    type=click.Path(),
    help="Directory path to populate from.",
)
@coro
async def populate(session_db: Path, target_dir: Path) -> None:
    """Populate the database with files from a target directory.


    Args:
        session_db (Path): Path to the session database.
        target_dir (Path): Directory path to populate from.
    """
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()

    async with db_handler.session_scope() as db_session:
        discovered_files: list[BkpFile] = []

        async def my_walker(
            dir_: AsyncPath,
            entry: AsyncPath,
            stat_result_i: os.stat_result,
            bkp_xml: AsyncBkpXml,
        ):
            bkp_file = bkp_xml[entry.name]
            assert bkp_file.md5
            discovered_files.append(bkp_file)

        walker = BackupXmlWalker(target_dir)
        await walker.go_walk(walker=my_walker)
        # Call the populate method
        await db_session.populate(discovered_files, async_copy_file)


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database.",
)
@click.option(
    "--restore-file",
    required=True,
    type=click.File("rb"),
    help="Path to restore File",
)
@coro
async def add_restore_context(session_db: Path, restore_file: IO) -> None:
    """Add a restore context to the backup database.

    Args:
        session_db (Path): Path to the session database.
        restore_file (Path): Directory path to restore from.
    """

    try:
        restore_context = RestoreContext.from_file_handle(restore_file)
    except FileNotFoundError:
        raise click.ClickException(f"Restore file {restore_file} not found.")
    except Exception as e:
        raise click.ClickException(
            f"An error occurred while reading the restore file: {e}"
        )
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        # Implement the logic to add the restore context
        await db_session.add_restore_context(restore_context)
        print(f"Restore context for {restore_file} added to the database.")


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database. (Defaults to usimg ~/.bkp_base)",
)
@click.option(
    "--target-dir",
    required=True,
    type=click.Path(),
    help="Directory path to backup to.",
)
@coro
async def discover(session_db: Path | None, target_dir: Path) -> None:
    """discover files already backed up to a destination
    Should be a seldom used bit of functionality
    e.g. if you have used rsync to backup the files already
    and want to make use of them
    Or can help for recovering from failures/corruption

    Args:
        session_db (Path): Path to the session database.
        dest_dir (Path): Directory path discover existing backed up files
    """
    vid_awaitable = VolumeIdSrc(target_dir).avolume_id
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        discovered_files: list[BkpFile] = []

        async def my_walker(
            dir_: AsyncPath,
            entry: AsyncPath,
            stat_result_i: os.stat_result,
            bkp_xml: AsyncBkpXml,
        ):
            bkp_file = bkp_xml[entry.name]
            assert bkp_file.md5
            discovered_files.append(bkp_file)

        walker = BackupXmlWalker(target_dir)
        await walker.go_walk(walker=my_walker)

        dest_id = await vid_awaitable
        # FIXME rather than have the intermediate list, we should be able to
        # have the callback function query the files in the database
        await db_session.discovery(files=discovered_files, dest=dest_id)
        await writeback_db_file_entries(db_session)


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database. (Defaults to usimg ~/.bkp_base)",
)
@click.option(
    "--target-dir",
    required=True,
    type=click.Path(),
    help="Directory path to backup to.",
)
@click.option(
    "--initialised",
    is_flag=True,
    help="The destination directory is expected to be initialised.",
)
@coro
async def target(
    session_db: Path | None, target_dir: Path, initialised: bool = False
) -> None:
    """target a destination for backup

    Args:
        session_db (Path): Path to the session database.
        target_dir (Path): Directory path to backup to.
        initialised (bool): If true, the destination directory is expected to be initialised.
    """
    if not target_dir.exists():
        raise click.ClickException(f"{target_dir} does not exist")

    # FIXME make use of initialised
    vid_awaitable = VolumeIdSrc(target_dir).avolume_id
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as bdsa:
        dest_id = await vid_awaitable
        async with AsyncBkpXmlManager() as bkp_xmls:
            await copy_best_files(bdsa, AsyncPath(target_dir), dest_id, bkp_xmls)

        # After all files are backed up, update the source directory entries
        await update_source_directory_entries(bdsa, dest_id)


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database. (Defaults to usimg ~/.bkp_base)",
)
@coro
async def update(session_db: Path | None) -> None:
    """Update the session database

    Args:
        session_db (Path): Path to the session database.
    """
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        # Implement the logic to update the session database
        # This will query the the database for the source directories
        # Then for each source directory, it will scan the directory for changes
        # creating/updating the .xml files for the changes
        for src in await db_session.aquery_generator(BackupSrc):
            await create_update_db_file_entries(db_session, src)

        await remove_unvisited_files_from_database(db_session)


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database. (Defaults to usimg ~/.bkp_base)",
)
@click.option(
    "--src-dir", required=True, type=click.Path(), help="Directory path to backup."
)
@coro
async def add_src(session_db: Path | None, src_dir: Path) -> None:
    """Add a Source directory for backup to our backup database
    This will add the directory to the database and scan it for changes

    Args:
        session_db (Path | None): _description_
        src_dir (Path): _description_
    """
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        await db_session.add_src_dir(src_dir)
        for src in await db_session.aquery_generator(BackupSrc):
            print(f"{src.path=} exists")


cli.add_command(populate)
cli.add_command(add_restore_context)
cli.add_command(discover)
cli.add_command(target)
cli.add_command(update)
cli.add_command(add_src)

if __name__ == "__main__":
    cli()
