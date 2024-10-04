import base64
import hashlib

import aiofiles


def md5_generator():
    """Generator to update MD5 hash with received chunks."""
    hash_md5 = hashlib.md5()
    chunk = yield
    while chunk:
        hash_md5.update(chunk)
        chunk = yield
    tmp = base64.b64encode(hash_md5.digest())
    return tmp[:22] if len(tmp) == 24 and tmp[22:24] == b"==" else tmp


def calculate_md5(file_path):
    """Calculate MD5 hash for a given file."""
    gen = md5_generator()
    next(gen)  # Initialize the generator
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            gen.send(chunk)
    try:
        return gen.send(None)
    except StopIteration as e:
        return e.value

async def async_calculate_md5(file_path):
    gen = md5_generator()
    next(gen)  # Initialize the generator
    async with aiofiles.open(file_path, "rb") as file:
        while chunk := await file.read(8192):
            gen.send(chunk)
    try:
        return gen.send(None)
    except StopIteration as e:
        return e.value
