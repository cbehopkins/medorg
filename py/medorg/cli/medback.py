from collections import defaultdict
import logging
import logging.config
import os
from pathlib import Path
from typing import IO, Optional

import click
from aiopath import AsyncPath
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from InquirerPy import prompt

from medorg.bkp_p.async_bkp_xml import AsyncBkpXml, AsyncBkpXmlManager
from medorg.bkp_p.backup_xml_walker import BackupXmlWalker
from medorg.cli import VERSION, coro
from medorg.cli.runners import (
    copy_best_files,
    create_update_db_file_entries,
    remove_unvisited_files_from_database,
    update_source_directory_entries,
    writeback_db_file_entries,
)
from medorg.common.bkp_file import BkpFile
from medorg.common.file_utils import async_copy_file
from medorg.common.types import BackupFile, BackupSrc


from medorg.database.database_handler import DatabaseHandler
from medorg.restore.structs import RestoreContext
from medorg.volume_id.volume_id import VolumeIdSrc


class ColourLevelFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        colour = self._record_colour(record.levelno)
        markup = f"[{colour}] \\[{record.levelname}] [/{colour}]"
        return markup + logging.Formatter.format(self, record)

    def _record_colour(self, levelno: int) -> str:
        colour = "blue"
        if levelno >= logging.INFO:
            colour = "bold green"
        if levelno >= logging.WARNING:
            colour = "bold dark_orange"
        if levelno >= logging.ERROR:
            colour = "bold red"
        return colour


DEFAULT_LOGGING_SETUP = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {"format": "[%(levelname)s] %(message)s"},
        "detailed": {
            "format": "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "myFormatter": {
            "()": "medorg.cli.medback.ColourLevelFormatter",
            "format": " %(message)s",
        },
    },
    "handlers": {
        "stdout": {
            "class": "rich.logging.RichHandler",
            "level": "CRITICAL",
            "formatter": "myFormatter",
            "show_level": False,
            "markup": True,
        },
        "null": {
            "class": "logging.NullHandler",
        },
    },
    "loggers": {"root": {"level": "DEBUG", "handlers": ["null"]}},
}
LOGGING_FILE_HANDLER = {
    "class": "logging.FileHandler",
    "level": "DEBUG",
    "formatter": "detailed",
    "mode": "w",
    "filename": "",
}


def bytes_to_human_readable(byte_count: int) -> str:
    """Convert a byte count into a human-readable format (e.g., KiB, MiB, GiB).

    Args:
        byte_count (int): The number of bytes.

    Returns:
        str: The human-readable format of the byte count.
    """
    suffixes = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
    i = 0
    while byte_count >= 1024 and i < len(suffixes) - 1:
        byte_count /= 1024.0
        i += 1
    return f"{byte_count:.2f} {suffixes[i]}"


@click.group()
@click.option("--log-level", default="CRITICAL")
@click.option("--log-file", default=None)
@click.version_option(VERSION)
def cli(log_level: Optional[str], log_file: Optional[str]):
    logging_setup = DEFAULT_LOGGING_SETUP.copy()
    if log_level:
        logging_setup["handlers"]["stdout"]["level"] = logging.getLevelName(log_level)
    if log_file:
        logging_setup["handlers"]["file"] = LOGGING_FILE_HANDLER.copy()
        logging_setup["handlers"]["file"]["filename"] = str(log_file)
        logging_setup["loggers"]["root"]["handlers"].append("file")
    logging.config.dictConfig(logging_setup)
    logging.basicConfig(level="INFO", handlers=[RichHandler()])


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database.",
)
@coro
async def backup_stats(session_db: Path) -> None:
    """Display backup statistics.
    That is, how well are our source files backed up

    Args:
        session_db (Path): Path to the session database.
    """
    db_handler = DatabaseHandler(session_db)
    console = Console()
    await db_handler.create_session()

    async with db_handler.session_scope() as db_session:
        table = Table(title="Backup Statistics")
        table.add_column(
            "Number of Destinations", justify="right", style="cyan", no_wrap=True
        )
        table.add_column("File Count", justify="right", style="magenta")

        backup_stats = db_session.count_files_by_backup_dest_length()
        for num_destinations, dest_count in backup_stats.items():
            table.add_row(str(num_destinations), str(dest_count))

        console.print(table)
        table = Table(title="Backup Statistics")
        table.add_column(
            "Number of Destinations", justify="right", style="cyan", no_wrap=True
        )
        table.add_column("Total Size (bytes)", justify="right", style="magenta")

        backup_stats = db_session.size_files_by_backup_dest_length()
        for num_destinations, total_size in backup_stats.items():
            table.add_row(str(num_destinations), bytes_to_human_readable(total_size))

        console.print(table)


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database.",
)
@coro
async def restore_stats(session_db: Path) -> None:
    """Display restore statistics.

    Args:
        session_db (Path): Path to the session database.
    """
    console = Console()
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()

    async with db_handler.session_scope() as db_session:
        missing_files_count = 0
        table = Table(title="Backup Statistics")
        dest_count_table = defaultdict(int)
        async for file in db_session.missing_files():
            missing_files_count += 1
            for dest in file.backup_dest:
                dest_count_table[dest.name] += 1

    table.add_column("Destination", justify="right", style="cyan", no_wrap=True)
    table.add_column("File Count", justify="right", style="magenta")
    for dest, count in dest_count_table.items():
        table.add_row(dest, str(count))
    console.print(table)
    console.print(f"Missing files: {missing_files_count}")


@click.command()
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
    This uses the inforation we have restored from the Restore context
    (Or what was already in the session file) to populate the files

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
    A restore context contains the file structure of the files we have backed up
    This command populates the database with what we expect to exist

    Args:
        session_db (Path): Path to the session database.
        restore_file (Path): Directory path to restore from.
    """

    try:
        restore_context = RestoreContext.from_file_handle(restore_file)
    except FileNotFoundError as e:
        raise click.ClickException(f"Restore file {restore_file} not found.") from e
    except Exception as e:
        raise click.ClickException(
            f"An error occurred while reading the restore file: {e}"
        ) from e
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
    help="Path to the session database.",
)
@click.option(
    "--restore-file",
    required=True,
    type=click.Path(),
    help="Path to restore File",
)
@coro
async def write_restore_context(session_db: Path, restore_file: Path) -> None:
    """Write a restore context from the backup database.

    Args:
        session_db (Path): Path to the session database.
        restore_file (Path): Path to file to write the restore context to.
    """

    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        restore_context = RestoreContext()
        await restore_context.build_file_structure(db_session)
        restore_file.write_text(restore_context.to_xml_string())


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
@coro
async def target(session_db: Path | None, target_dir: Path) -> None:
    """target a destination for backup
    Run this command after populating the database with files
    to copy the files to the destination

    Args:
        session_db (Path): Path to the session database.
        target_dir (Path): Directory path to backup to.
    """
    if not target_dir.exists():
        raise click.ClickException(f"{target_dir} does not exist")

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
    help="Path to the session database. (Defaults to using ~/.bkp_base)",
)
@click.option("--interactive", is_flag=True, help="Run the update in interactive mode.")
@coro
async def update(session_db: Path | None, interactive: bool) -> None:
    """Update the session database

    Args:
        session_db (Path): Path to the session database.
        interactive (bool): Run the update in interactive mode.
    """
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        sources = [src async for src in db_session.aquery_generator(BackupSrc)]

        if interactive:
            choices = [{"name": str(src), "value": src} for src in sources]
            questions = [
                {
                    "type": "checkbox",
                    "message": "Select sources to update",
                    "name": "selected_sources",
                    "choices": choices,
                }
            ]
            answers = prompt(questions)
            selected_sources = answers.get("selected_sources", [])
        else:
            selected_sources = sources

        for src in selected_sources:
            await create_update_db_file_entries(db_session, src)

        await remove_unvisited_files_from_database(db_session)


@click.command()
@click.option(
    "--session-db",
    required=False,
    type=click.Path(),
    help="Path to the session database. (Defaults to usimg ~/.bkp_base)",
)
@click.argument(
    "src_dirs",
    nargs=-1,
    type=click.Path(),
)
@coro
async def add_src(session_db: Path | None, src_dirs: list[Path]) -> None:
    """Add a Source directory for backup to our backup database
    This will add the directory to the database and scan it for changes

    Args:
        session_db (Path | None): _description_
        src_dirs (list[Path]): List of directory paths to backup.
    """
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    async with db_handler.session_scope() as db_session:
        for src_dir in src_dirs:
            await db_session.add_src_dir(src_dir)
        for src in await db_session.aquery_generator(BackupSrc):
            print(f"{src.path=} exists")


cli.add_command(backup_stats)
cli.add_command(restore_stats)
cli.add_command(populate)
cli.add_command(add_restore_context)
cli.add_command(write_restore_context)
cli.add_command(discover)
cli.add_command(target)
cli.add_command(update)
cli.add_command(add_src)

if __name__ == "__main__":
    cli()
