# Stolen from https://github.com/pallets/click/issues/85
import asyncio
from functools import wraps

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

try:
    VERSION = importlib_metadata.version("medorg")
except importlib_metadata.PackageNotFoundError:
    # Running from source tree (e.g., tox) without installed dist metadata.
    VERSION = "0.0.0"


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper
