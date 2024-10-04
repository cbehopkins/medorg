# Stolen from https://github.com/pallets/click/issues/85
import asyncio
from functools import wraps
try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

VERSION = importlib_metadata.version("medorg")


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper
