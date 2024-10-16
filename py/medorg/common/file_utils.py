import shutil
from pathlib import Path

from aiopath import AsyncPath


async def async_copy():
    ...
    # create file objects for the source and destination
    # handle_src = await aiofiles.open('files_copy.txt', mode='r')
    # handle_dst = await aiofiles.open('files_copy2.txt', mode='w')
    # # get the number of bytes for the source
    # stat_src = await aiofiles.os.stat('files_copy.txt')
    # n_bytes = stat_src.st_size
    # # get the file descriptors for the source and destination files
    # fd_src = handle_src.fileno()
    # fd_dst = handle_dst.fileno()
    # # copy the file
    # await aiofiles.os.sendfile(fd_dst, fd_src, 0, n_bytes)


async def async_copy_file(src: AsyncPath, dest: AsyncPath):
    # FIXME find an awaitable version of this
    try:
        shutil.copy2(str(src), str(dest))
    except Exception as e:
        print(e)
        raise


def path_from_dirs(dirs, file):
    if len(dirs) == 0:
        return Path(file)
    pth = dirs[0]
    for p in dirs[1:]:
        pth = pth / p
    return pth / file
