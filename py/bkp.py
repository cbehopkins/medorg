import argparse
import asyncio
import logging
from pathlib import Path
from bkp_p.bdsa import DatabaseHandler

from bkp_p.runners import backup_files
from bkp_p.volume_id import VolumeIdSrc

_log = logging.getLogger(__name__)


def log_level_to_int(level):
    try:
        level = int(level)
    except ValueError:
        level = logging.getLevelName(level)
    return level


def configure_logging(log_level, logfile):
    logging_config = {"level": log_level}
    if logfile:
        print(f"Logging to {logfile}")
        logging_config["filename"] = logfile
        logging_config["encoding"] = "utf-8"

    logging.basicConfig(**logging_config)

async def main():
    parser = argparse.ArgumentParser(
        description="Backup files from source directory to destination directory."
    )
    parser.add_argument("--source", type=Path, help="Source directory path")
    parser.add_argument("--destination", type=Path, help="Destination directory path")
    parser.add_argument(
        "--loglevel",
        default=logging.INFO,
        help="set the log level",
        type=log_level_to_int,
    )
    parser.add_argument("--logfile", type=Path)
    args = parser.parse_args()
    configure_logging(args.loglevel, args.logfile)

    src_dir = args.source.resolve()
    dest_dir = args.destination.resolve()

    vid_awaitable = VolumeIdSrc(dest_dir).avolume_id

    db_handler = DatabaseHandler(dest_dir)
    await db_handler.create_session()
    await db_handler.clear_files()
    dest_id = await vid_awaitable
    async with db_handler.session_scope() as db_session:
        await db_session.init_dest(dest_id)
        await backup_files(db_session, src_dir, dest_dir, dest_id)


if __name__ == "__main__":
    asyncio.run(main())
