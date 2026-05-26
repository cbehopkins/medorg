#!/usr/bin/env pwsh
# Dev task runner for medorg
# Usage: ./dev.ps1 [command] [-BaseBinDir <path>]

param(
    [Parameter(Position=0)]
    [string]$Command = "help",
    [string]$BaseBinDir
)

$ErrorActionPreference = "Stop"

$tasks = @{
    "help" = "Display this help message"
    "test" = "Run all tests"
    "test-race" = "Run tests with race condition detection"
    "test-coverage" = "Run tests with coverage report"
    "lint" = "Run static checks (go vet)"
    "fmt" = "Format code (gofmt -w .)"
    "fmt-check" = "Check if code needs formatting"
    "build" = "Build project via buildall.ps1"
    "release" = "Cross-platform release build via buildall.ps1"
    "build-local" = "Build project locally (go build ./cmd/... )"
    "clean" = "Clean build/test artifacts"
    "check" = "Run fmt-check, lint, and test"
    "all" = "Run fmt, lint, and test"
}

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Action
    )

    Write-Host "==> $Name" -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed"
    }
}

function Show-Help {
    Write-Host "Available tasks:" -ForegroundColor Cyan
    $tasks.GetEnumerator() | Sort-Object Name | ForEach-Object {
        Write-Host ("  {0,-20} {1}" -f $_.Name, $_.Value)
    }
    Write-Host ""
    Write-Host "Usage: ./dev.ps1 [command] [-BaseBinDir <path>]" -ForegroundColor Yellow
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  ./dev.ps1 test"
    Write-Host "  ./dev.ps1 build -BaseBinDir C:/tools/medorg"
}

function Invoke-Test {
    Invoke-Step "Running tests" { go test -v ./... }
}

function Invoke-TestRace {
    Invoke-Step "Running tests with race detection" { go test -v -race ./... }
}

function Invoke-TestCoverage {
    Invoke-Step "Running tests with coverage" { go test -v -coverprofile=coverage.out ./... }
    Invoke-Step "Generating coverage report" { go tool cover -html=coverage.out -o coverage.html }
    Write-Host "Coverage report: coverage.html" -ForegroundColor Green
}

function Invoke-Lint {
    Invoke-Step "Running go vet" { go vet ./... }
}

function Invoke-Fmt {
    Invoke-Step "Formatting code" { gofmt -w . }
}

function Invoke-FmtCheck {
    Write-Host "==> Checking code format" -ForegroundColor Cyan
    $unformatted = gofmt -l .
    if ($unformatted) {
        Write-Host "Go files need formatting:" -ForegroundColor Red
        $unformatted | ForEach-Object { Write-Host $_ }
        Write-Host "Run './dev.ps1 fmt' to fix." -ForegroundColor Yellow
        exit 1
    }
    Write-Host "Code format OK" -ForegroundColor Green
}

function Invoke-Build {
    $buildScript = Join-Path $PSScriptRoot "buildall.ps1"
    if (-not (Test-Path $buildScript)) {
        throw "buildall.ps1 not found at $buildScript"
    }

    if ($BaseBinDir) {
        Invoke-Step "Running buildall.ps1 with BaseBinDir=$BaseBinDir" { & $buildScript -BaseBinDir $BaseBinDir }
    } else {
        Invoke-Step "Running buildall.ps1" { & $buildScript }
    }
}

function Invoke-Release {
    $buildScript = Join-Path $PSScriptRoot "buildall.ps1"
    if (-not (Test-Path $buildScript)) {
        throw "buildall.ps1 not found at $buildScript"
    }

    $releaseBaseBinDir = if ($BaseBinDir) { $BaseBinDir } elseif ($env:MEDORG_RELEASE_BIN_DIR) { $env:MEDORG_RELEASE_BIN_DIR } else { "$HOME/home/git" }
    Invoke-Step "Running release build via buildall.ps1 (BaseBinDir=$releaseBaseBinDir)" { & $buildScript -BaseBinDir $releaseBaseBinDir }
}

function Invoke-BuildLocal {
    Invoke-Step "Building local commands" { go build ./cmd/... }
}

function Invoke-Clean {
    Invoke-Step "Cleaning test cache" { go clean -testcache }
    Remove-Item -Force -ErrorAction SilentlyContinue coverage.out, coverage.html
    Write-Host "Clean complete" -ForegroundColor Green
}

function Invoke-Check {
    Write-Host "Running full check (fmt-check, lint, test)..." -ForegroundColor Cyan
    Invoke-FmtCheck
    Invoke-Lint
    Invoke-Test
    Write-Host "All checks passed!" -ForegroundColor Green
}

function Invoke-All {
    Write-Host "Running all tasks (fmt, lint, test)..." -ForegroundColor Cyan
    Invoke-Fmt
    Invoke-Lint
    Invoke-Test
    Write-Host "All tasks completed!" -ForegroundColor Green
}

switch ($Command) {
    "help" { Show-Help }
    "test" { Invoke-Test }
    "test-race" { Invoke-TestRace }
    "test-coverage" { Invoke-TestCoverage }
    "lint" { Invoke-Lint }
    "fmt" { Invoke-Fmt }
    "fmt-check" { Invoke-FmtCheck }
    "build" { Invoke-Build }
    "release" { Invoke-Release }
    "build-local" { Invoke-BuildLocal }
    "clean" { Invoke-Clean }
    "check" { Invoke-Check }
    "all" { Invoke-All }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Show-Help
        exit 1
    }
}
