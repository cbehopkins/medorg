from os import PathLike
from pathlib import Path

from medorg.common.checksum import calculate_md5
from medorg.common.types import Checksum


def to_sync_path(path_value: PathLike[str] | str) -> Path:
    """Explicitly convert path-like values into a synchronous pathlib.Path."""
    return Path(str(path_value))


def require_existing_file(path_value: PathLike[str] | str) -> Path:
    """Boundary helper for sync I/O callers that need a local file path."""
    sync_path = to_sync_path(path_value)
    if not sync_path.is_file():
        raise FileNotFoundError(f"File {sync_path} not found")
    return sync_path


def calculate_md5_for_existing_file(path_value: PathLike[str] | str) -> Checksum:
    """Calculate md5 at a sync boundary after explicit path normalization."""
    sync_path = require_existing_file(path_value)
    return calculate_md5(str(sync_path))
