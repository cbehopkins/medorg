from datetime import datetime
import logging
from pathlib import Path
from typing import Iterable, Union
from aiopath import AsyncPath

from sqlalchemy import select

from medorg.common.bkp_file import BkpFile
from medorg.common.types import BackupDest, BackupFile, BackupSrc, VolumeId
from medorg.database.bdsa import Bdsa


_log = logging.getLogger(__name__)


async def query_src_dir(bdsa: Bdsa, src_dir: Path) -> BackupSrc:
    src_dir_r = await AsyncPath(src_dir).resolve()
    _log.debug(f"Resolving {src_dir} to {src_dir_r} for query")
    result = (
        (
            await bdsa.session.execute(
                select(BackupSrc).filter(BackupSrc.path == str(src_dir_r))
            )
        )
        .scalars()
        .all()
    )
    if not result:
        return None
    assert len(result) == 1
    return result[0]


async def query_all_files(bdsa: Bdsa) -> list[BackupFile]:
    async with bdsa._lock:
        result = await bdsa.session.execute(select(BackupFile))
    return result.unique().scalars().all()


async def aquery_all_files(self) -> list[BackupFile]:
    return list(await self.aquery_generator(BackupFile, None))


async def query_hash(bdsa: Bdsa, hash: str) -> list[BackupFile] | None:
    result = await bdsa.aquery_generator(BackupFile, BackupFile.md5_hash == hash)
    if not result:
        return None
    return list(result)


async def query_files_without_dest(bdsa: Bdsa, dest_name: VolumeId) -> list[BackupFile]:
    return list(
        await bdsa.aquery_generator(
            BackupFile, ~BackupFile.backup_dest.any(BackupDest.name == dest_name)
        )
    )


async def visit_files(bdsa: Bdsa, props_src: Iterable[BkpFile], src_dir: AsyncPath):
    for props in props_src:
        entry = await bdsa.update_file(props, src_dir=src_dir)
        entry.visited = 1


async def query_dest(bdsa, dest_name: VolumeId) -> list[BackupFile]:
    result = await bdsa.aquery_generator(
        BackupFile, BackupFile.backup_dest.any(name=dest_name)
    )
    return list(result)


async def aquery_all_src_dirs(self) -> list[BackupSrc, None]:
    return list(await self.aquery_generator(BackupSrc))


async def query_files_visited(bdsa) -> list[BackupFile]:
    # Query for files That we visited this run
    return list(await bdsa.aquery_generator(BackupFile, BackupFile.visited != 0))


async def add_file(
    bdsa: Bdsa,
    file_name: str,
    dest_names: Union[VolumeId, list[VolumeId]] = [""],
    size: int = 0,
    timestamp: Union[int] = 0,
    md5_hash: str = "",
    visited: bool = False,
    src_path: str = "./",
) -> None:
    # This should only really be used in testbenches to setup files...
    if isinstance(timestamp, int):
        timestamp = datetime.fromtimestamp(timestamp)
    assert not AsyncPath(file_name).is_absolute()
    # Add a new file with a specified tag
    new_file = BackupFile(
        filename=file_name,
        src_path=src_path,
        size=size,
        timestamp=timestamp,
        md5_hash=md5_hash,
        visited=int(visited),
    )
    if dest_names:
        if not isinstance(dest_names, list):
            assert isinstance(dest_names, str)
            dest_names = [dest_names]
        await bdsa.add_bkp_dests_to_backup_file(dest_names, new_file)

    await bdsa.add(new_file)
