from pathlib import Path

import pytest
from aiopath import AsyncPath

from medorg.common.io_boundary import (
    calculate_md5_for_existing_file,
    require_existing_file,
    to_sync_path,
)


def test_to_sync_path_from_path():
    path = Path("abc") / "def.txt"
    assert to_sync_path(path) == path


def test_to_sync_path_from_asyncpath():
    async_path = AsyncPath("abc") / "def.txt"
    assert to_sync_path(async_path) == Path("abc") / "def.txt"


def test_require_existing_file_success(tmp_path):
    p = tmp_path / "file.txt"
    p.write_text("hello")
    resolved = require_existing_file(p)
    assert resolved == p


def test_require_existing_file_missing(tmp_path):
    missing = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError, match="not found"):
        require_existing_file(missing)


def test_calculate_md5_for_existing_file(tmp_path):
    p = tmp_path / "file.txt"
    p.write_text("hello")
    md5_value = calculate_md5_for_existing_file(p)
    assert isinstance(md5_value, (str, bytes))
    assert md5_value
