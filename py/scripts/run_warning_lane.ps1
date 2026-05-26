Push-Location $PSScriptRoot\..
try {
    poetry run pytest tests/test_cli.py tests/test_file_access.py tests/test_bdsa.py `
        -W "error:coroutine 'AsyncPath.stat' was never awaited:RuntimeWarning" `
        -W "error::_pytest.warning_types.PytestUnraisableExceptionWarning"
}
finally {
    Pop-Location
}
