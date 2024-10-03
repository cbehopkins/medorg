import base64
import hashlib

import aiofiles


def calculate_md5(file_path):
    """Calculate MD5 hash for a given file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    tmp = base64.b64encode(hash_md5.digest())
    if len(tmp) == 24 and tmp[22:24] == b"==":
        return tmp[:22]
    return tmp


async def async_calculate_md5(file_path):
    md5 = hashlib.md5()
    async with aiofiles.open(file_path, "rb") as file:
        while chunk := await file.read(8192):
            md5.update(chunk)
    tmp = base64.b64encode(md5.digest())
    if len(tmp) == 24 and tmp[22:24] == b"==":
        return tmp[:22]
    return tmp
