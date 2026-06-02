#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

uvx tox -e py312 -- tests/test_cli.py tests/test_file_access.py tests/test_bdsa.py \
	-W "error:coroutine 'AsyncPath.stat' was never awaited:RuntimeWarning" \
	-W "error::_pytest.warning_types.PytestUnraisableExceptionWarning"
