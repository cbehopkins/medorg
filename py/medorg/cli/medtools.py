import logging
import os
from pathlib import Path

import click
from aiopath import AsyncPath

from medorg.bkp_p.async_bkp_xml import AsyncBkpXml
from medorg.bkp_p.backup_xml_walker import BackupXmlWalker
from medorg.cli import VERSION, coro
from medorg.common.types import BackupFile
from medorg.database.database_handler import DatabaseHandler

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)


@click.group()
@click.version_option(VERSION)
def cli():
    pass


@cli.command()
@click.argument("directory", type=click.Path(exists=True))
@coro
async def generate(directory: Path):
    """Walk a directory tree and log each file found."""

    async def my_walker(
        dir_: AsyncPath,
        entry: AsyncPath,
        stat_result_i: os.stat_result,
        bkp_xml: AsyncBkpXml,
    ):
        _log.info(f"Processing {entry.name} in {dir_}")

    walker = BackupXmlWalker(directory)
    await walker.go_walk(walker=my_walker)


@cli.command()
@click.argument("--session-db", required=False, type=click.Path())
@coro
async def md5_clash(session_db: Path):
    """Check for MD5 clashes in the session database."""
    db_handler = DatabaseHandler(session_db)
    await db_handler.create_session()
    found_hashes: dict[tuple[str, int], BackupFile] = {}
    async with db_handler.session_scope() as db_session:
        file: BackupFile
        async for file in db_session.aquery_generator(BackupFile):
            key = (file.md5_hash, file.size)
            if key in found_hashes:
                _log.info(
                    f"Clash found for {file.md5_hash}::{file}, {found_hashes[file.md5_hash]}"
                )
            found_hashes[key] = file


if __name__ == "__main__":
    cli()
