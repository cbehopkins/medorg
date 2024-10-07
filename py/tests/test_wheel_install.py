import os
import subprocess
import venv
from pathlib import Path

import pytest


def run_command(command, env=None):
    result = subprocess.run(
        command, capture_output=True, text=True, shell=True, env=env
    )
    if result.returncode != 0:
        print(
            f"Command {' '.join(command)} failed with return code {result.returncode}"
        )
        print(result.stdout)
        print(result.stderr)
        pytest.fail(f"Command {''.join(command)} failed")
    return result


def run_command_in_venv(command, venv_path):
    if os.name == "nt":
        activate_script = os.path.join(venv_path, "Scripts", "activate.bat")
        full_command = f'cmd.exe /c "{activate_script} & {command}"'
    else:
        activate_script = os.path.join(venv_path, "bin", "activate")
        full_command = f'bash -c "source {activate_script} && {command}"'

    return subprocess.run(full_command, shell=True, capture_output=True, text=True)


def test_install_cli(tmp_path):
    pytest.skip("It's a long test, skip it for now")
    # Create a virtual environment
    venv_dir = tmp_path / "venv"
    venv.create(venv_dir, with_pip=True)

    # Determine the path to the virtual environment's Python executable
    if os.name == "nt":
        python_executable = venv_dir / "Scripts" / "python.exe"
    else:
        python_executable = venv_dir / "bin" / "python"

    # Build the package
    run_command("poetry build")

    # Find the built wheel file
    dist_dir = Path("dist")
    wheel_files = [f for f in dist_dir.iterdir() if f.suffix == ".whl"]
    assert wheel_files, "No wheel file found in dist directory"
    wheel_file = wheel_files[0]

    # Install the package in the virtual environment
    run_command(f"{python_executable} -m pip install {wheel_file}")

    _run_test_one_command("medback --help", venv_dir)
    _run_test_one_command("medtools --help", venv_dir)


def _run_test_one_command(arg0, venv_dir):
    # Run the mdback CLI command using subprocess
    result = run_command_in_venv(arg0, venv_dir)
    assert result.returncode == 0
    # Check if the output contains the expected help message
    assert (
        "Usage:" in result.stdout
    ), "CLI command did not produce the expected help message"

    return result


if __name__ == "__main__":
    pytest.main()
