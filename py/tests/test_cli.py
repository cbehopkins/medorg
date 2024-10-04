import base64
import hashlib
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from unittest import mock

import pytest
from click.testing import CliRunner

from medorg.bkp_p import XML_NAME
from medorg.cli.medback import cli
from medorg.database.bdsa import Bdsa
from medorg.database.database_handler import DatabaseHandler
from medorg.restore.structs import (RestoreContext, RestoreDirectory,
                                    RestoreFile)
from medorg.volume_id.volume_id import VolumeIdSrc


@dataclass
class ExampleFile:
    path: Path
    content: str

    def __post_init__(self):
        self.path.write_text(self.content)

    def calculate_md5(self) -> str:
        """Calculate MD5 hash for the file content."""
        hash_md5 = hashlib.md5()
        hash_md5.update(self.content.encode("utf-8"))
        tmp = base64.b64encode(hash_md5.digest())
        if len(tmp) == 24 and tmp[22:24] == b"==":
            return tmp[:22].decode("utf-8")
        return tmp.decode("utf-8")

TestFiles = tuple[list[ExampleFile], set[Path]]


@pytest.fixture
def example_files(tmp_path: Path) -> TestFiles:
    # Define the dataset for test files
    test_files = [
        ("subdir1/file1.txt", "This is test file 1."),
        ("subdir1/file2.txt", "This is test file 2."),
        ("subdir2/file3.txt", "This is test file 3."),
    ]

    # Create the subdirectories
    subdirs = {Path(file[0]).parent for file in test_files}
    for subdir in subdirs:
        (tmp_path / subdir).mkdir(parents=True, exist_ok=True)

    # Construct the ExampleFile instances
    files = [
        ExampleFile(path=tmp_path / Path(file[0]), content=file[1])
        for file in test_files
    ]

    return files, subdirs


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, f"Error: {result.output}"


def test_cli_add_src_update(tmp_path: Path, example_files: TestFiles) -> None:
    files, subdirs = example_files
    session_db = tmp_path / "session"
    runner = CliRunner()
    for dir_ in subdirs:
        result = runner.invoke(
            cli,
            ["add-src", "--session-db", session_db, "--src-dir", str(tmp_path / dir_)],
        )
        assert result.exit_code == 0, f"Error: {result.output}"
    result = runner.invoke(cli, ["update", "--session-db", session_db])

    assert result.exit_code == 0, f"Error: {result.output}"
    # Check for XML files in each subdirectory
    for subdir in subdirs:
        xml_file = tmp_path / subdir / XML_NAME
        assert xml_file.exists()

        # Verify XML content
        tree = ET.parse(xml_file)
        root = tree.getroot()
        for file in files:
            if file.path.parts[-2] == subdir.name:
                file_element = root.find(f"./fr[@fname='{file.path.name}']")
                assert file_element is not None
                assert file_element.attrib["checksum"] == file.calculate_md5()
                assert int(file_element.attrib["size"]) == len(file.content)


def _copy_subdirs_to_target(subdirs: Sequence[Path], target_dir: Path) -> None:
    """
    Recursively copy the contents of subdirs into target_dir.

    :param subdirs: List of directories to copy from.
    :param target_dir: Directory to copy to.
    """
    target_dir = Path(target_dir)
    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)

    for subdir in subdirs:
        subdir = Path(subdir)
        if subdir.is_dir():
            for item in subdir.iterdir():
                dest = target_dir / item.name
                if item.is_dir():
                    shutil.copytree(item, dest, dirs_exist_ok=True)
                else:
                    shutil.copy2(item, dest)


def test_target_discovery(tmp_path: Path, example_files: TestFiles) -> None:
    files, subdirs = example_files
    target_dir = tmp_path / "target"
    session_db = tmp_path / "session"
    _copy_subdirs_to_target([tmp_path / d for d in subdirs], target_dir)

    runner = CliRunner()
    # Let's now pretend we are about to do a new fancy backup
    for dir_ in subdirs:
        result = runner.invoke(
            cli,
            ["add-src", "--session-db", session_db, "--src-dir", str(tmp_path / dir_)],
        )
        assert result.exit_code == 0, f"Error: {result.output}"
    result = runner.invoke(cli, ["update", "--session-db", session_db])
    assert result.exit_code == 0, f"Error: {result.output}"
    result = runner.invoke(
        cli, ["discover", "--session-db", session_db, "--target-dir", str(target_dir)]
    )
    assert result.exit_code == 0, f"Error: {result.output}"

    volume_id = VolumeIdSrc(target_dir).volume_id
    for subdir in subdirs:
        xml_file = tmp_path / subdir / XML_NAME
        assert xml_file.exists()

        # Verify XML content
        tree = ET.parse(xml_file)
        root = tree.getroot()
        for file in files:
            if file.path.parts[-2] == subdir.name:
                file_element = root.find(f"./fr[@fname='{file.path.name}']")
                assert len(file_element) == 1
                destinations = {fe.attrib["id"] for fe in file_element}
                assert volume_id in destinations


@mock.patch("medorg.cli.runners.async_copy_file")
def test_target_backup_some_files(mock_copy, tmp_path: Path, example_files) -> None:
    files, subdirs = example_files
    session_db = tmp_path / "session"
    target_dir_0 = tmp_path / "target_0"
    target_dir_1 = tmp_path / "target_1"
    target_dir_0.mkdir()
    target_dir_1.mkdir()
    runner = CliRunner()

    for dir_ in subdirs:
        result = runner.invoke(
            cli,
            ["add-src", "--session-db", session_db, "--src-dir", str(tmp_path / dir_)],
        )
        assert result.exit_code == 0, f"add-src Error: {result.output}"
    result = runner.invoke(cli, ["update", "--session-db", session_db])
    assert result.exit_code == 0, f"update Error: {result.output}"
    result = runner.invoke(
        cli, ["target", "--session-db", session_db, "--target-dir", target_dir_0]
    )
    assert result.exit_code == 0, f"target Error: {result.output}"
    assert len(mock_copy.await_args_list) == len(files)

    # Test that if we run the target command again, no files are copied
    result = runner.invoke(
        cli, ["target", "--session-db", session_db, "--target-dir", target_dir_0]
    )
    assert result.exit_code == 0, f"target Error: {result.output}"
    assert len(mock_copy.await_args_list) == len(files)

    # Test that if we backup to a new target we get another lot of copies
    result = runner.invoke(
        cli, ["target", "--session-db", session_db, "--target-dir", target_dir_1]
    )
    assert result.exit_code == 0, f"target Error: {result.output}"
    assert len(mock_copy.await_args_list) == len(files) * 2


def test_delete_src_files_between_backups(tmp_path: Path, example_files):
    files, subdirs = example_files
    session_db = tmp_path / "session"
    target_dir_0 = tmp_path / "target_0"
    target_dir_0.mkdir()
    target_dir_1 = tmp_path / "target_1"
    target_dir_1.mkdir()
    runner = CliRunner()
    for dir_ in subdirs:
        result = runner.invoke(
            cli,
            ["add-src", "--session-db", session_db, "--src-dir", str(tmp_path / dir_)],
        )
        assert result.exit_code == 0, f"target Error: {result.output}"
    result = runner.invoke(cli, ["update", "--session-db", session_db])
    assert result.exit_code == 0, f"target Error: {result.output}"
    result = runner.invoke(
        cli, ["target", "--session-db", session_db, "--target-dir", target_dir_0]
    )
    assert result.exit_code == 0, f"target Error: {result.output}"
    # Delete some source files
    files_to_delete = [files[0].path, files[1].path]
    for file in files_to_delete:
        file.unlink()

    runner.invoke(cli, ["update", "--session-db", session_db])
    assert result.exit_code == 0, f"target Error: {result.output}"

    result = runner.invoke(
        cli, ["target", "--session-db", session_db, "--target-dir", target_dir_1]
    )
    assert result.exit_code == 0, f"target Error: {result.output}"

    # Check that deleted files are not copied
    for file in files_to_delete:
        assert not (target_dir_1 / file.relative_to(tmp_path)).exists()


def test_create_new_files_in_source(tmp_path: Path, example_files):
    files, subdirs = example_files
    session_db = tmp_path / "session"
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    runner = CliRunner()
    for dir_ in subdirs:
        result = runner.invoke(
            cli,
            ["add-src", "--session-db", session_db, "--src-dir", str(tmp_path / dir_)],
        )
        assert result.exit_code == 0, f"add-src Error: {result.output}"
    result = runner.invoke(cli, ["update", "--session-db", session_db])
    assert result.exit_code == 0, f"update Error: {result.output}"
    # Create new files in the source directories
    new_files = [
        ExampleFile(
            path=tmp_path / "subdir1/new_file1.txt", content="This is new file 1."
        ),
        ExampleFile(
            path=tmp_path / "subdir2/new_file2.txt", content="This is new file 2."
        ),
    ]
    result = runner.invoke(cli, ["update", "--session-db", session_db])
    assert result.exit_code == 0, f"update Error: {result.output}"

    result = runner.invoke(
        cli, ["target", "--session-db", session_db, "--target-dir", target_dir]
    )
    assert result.exit_code == 0, f"target Error: {result.output}"

    # Check that new files are copied
    for file in new_files:
        expected_full_path = target_dir / file.path.relative_to(tmp_path)
        assert expected_full_path.exists(), expected_full_path


@pytest.fixture
def restore_context_from_example_files(tmp_path, example_files) -> RestoreContext:
    files, _ = example_files

    def add_file_to_structure(
        root: RestoreDirectory, file_path: Path, content: str, md5: str
    ):
        path = file_path.relative_to(tmp_path)
        parts = list(path.parts)
        current_dir = root
        for part in parts[:-1]:
            subdir = next(
                (d for d in current_dir.subdirectories if d.name == part), None
            )
            if subdir is None:
                subdir = RestoreDirectory(part)
                current_dir.add_subdirectory(subdir)
            current_dir = subdir
        restore_file = RestoreFile(
            name=parts[-1],
            size=len(content),
            file_path=file_path,
            md5=md5,
            bkp_dests=set(),
        )
        current_dir.add_file(restore_file)

    root_directory = RestoreDirectory("my_path")
    for example_file in files:
        add_file_to_structure(
            root_directory,
            example_file.path,
            example_file.content,
            example_file.calculate_md5(),
        )
    rc = RestoreContext(bdsa=None)
    rc.file_structure["my_path"] = root_directory
    return rc


@mock.patch("medorg.database.bdsa.AsyncSessionWrapper._session_add")
def test_add_restore_context_cli(
    mock_add, tmp_path: Path, restore_context_from_example_files: RestoreContext
):
    session_db = tmp_path / "session"
    restore_dir = tmp_path / "restore"
    restore_dir.mkdir()

    # Write the XML for the RestoreContext to a file
    xml_file = restore_dir / "restore_context.xml"
    with open(xml_file, "w") as f:
        f.write(restore_context_from_example_files.to_xml_string())

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "add-restore-context",
            "--session-db",
            str(session_db),
            "--restore-file",
            str(xml_file),
        ],
    )
    assert result.exit_code == 0, f"add_restore_context Error: {result.output}"

    def find_call(file: RestoreFile, tp: Path):
        rel_path = file.file_path.relative_to(tp)
        return any(
            Path(call[0][0].filename) == rel_path for call in mock_add.call_args_list
        )

    # Verify that Bdsa.session.add was called for each file in the restore context
    def check_dir(directory, tp):
        for file in directory.files:
            assert find_call(file, tp), f"Unable to find {file.file_path.name}"
        for subdir in directory.subdirectories:
            check_dir(subdir, tp)

    for directory in restore_context_from_example_files.file_structure.values():
        check_dir(directory, tmp_path)
