from pathlib import Path

import pytest
from aiopath import AsyncPath

from medorg.cli.runners import BackupFile, generate_src_dest_full_paths


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filename, make_abs, src_structure, expected_src, expected_dest",
    [
        (
            "subdir/file.txt",
            False,
            ["subdir/file.txt"],
            "subdir/file.txt",
            "bob/subdir/file.txt",
        ),
        (
            "home/bob/subdir/file.txt",
            True,
            ["subdir/file.txt"],
            "subdir/file.txt",
            "bob/subdir/file.txt",
        ),
        ("nonexistent/file.txt", False, [], None, None),
    ],
)
async def test_generate_src_dest_full_paths(
    tmp_path: Path,
    filename: str,
    make_abs: bool,
    src_structure: list[str],
    expected_src: str | None,
    expected_dest: str | None,
):
    # Setup
    src_path = tmp_path / "home" / "bob"
    src_path.mkdir(parents=True)
    if make_abs:
        filename = tmp_path / filename
    for file in src_structure:
        file_path = src_path / file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()

    dest_path = tmp_path / "mnt" / "drv0"
    dest_path.mkdir(parents=True)

    file_entry = BackupFile(filename=filename, src_path=str(src_path))

    # Call and Assertions
    if expected_src is None:
        with pytest.raises(FileNotFoundError):
            await generate_src_dest_full_paths(file_entry, dest_path)
    else:
        src_file_path, dest_file_path = await generate_src_dest_full_paths(
            file_entry, AsyncPath(dest_path)
        )
        assert src_file_path == src_path / expected_src
        assert dest_file_path == dest_path / expected_dest
        assert src_file_path.is_absolute()
        assert src_file_path.is_file()
