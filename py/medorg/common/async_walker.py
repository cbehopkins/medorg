# Nicked wholesale from https://github.com/Tinche/aiofiles/issues/167 until implemented...
import os

import aiofiles.os


async def _walk(top, onerror, followlinks):
    dirs = []
    nondirs = []

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    try:
        scandir_it = await aiofiles.os.scandir(top)
    except OSError as error:
        if onerror is not None:
            onerror(error)
        return

    with scandir_it:
        while True:
            try:
                try:
                    entry = next(scandir_it)
                except StopIteration:
                    break
            except OSError as error:
                if onerror is not None:
                    onerror(error)
                return

            try:
                is_dir = entry.is_dir()
            except OSError:
                # If is_dir() raises an OSError, consider that the entry is not
                # a directory, same behaviour than os.path.isdir().
                is_dir = False

            if is_dir:
                dirs.append(entry.name)
            else:
                nondirs.append(entry.name)

    yield top, dirs, nondirs

    # Recurse into sub-directories
    islink, join = aiofiles.os.path.islink, os.path.join
    for dirname in dirs:
        new_path = join(top, dirname)

        if followlinks or not await islink(new_path):
            # Change to yield from?
            async for x in _walk(new_path, onerror, followlinks):
                yield x


async def walk(top, onerror=None, followlinks=False):
    async for top, dirs, nondirs in _walk(top, onerror, followlinks):
        yield top, dirs, nondirs
