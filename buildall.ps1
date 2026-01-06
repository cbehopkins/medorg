#!/usr/bin/env pwsh
param(
    [string]$BaseBinDir
)

$ErrorActionPreference = "Stop"

# Set default if not provided
if (-not $BaseBinDir) {
    $BaseBinDir = if ($env:BASE_BIN_DIR) { $env:BASE_BIN_DIR } else { "$HOME/home/git" }
}

# Install for native OS
Write-Host "Installing for native OS..."
go install ./cmd/...

# Define build targets (OS/ARCH pairs and their output subdirectories)
$Targets = @{
    "linux/amd64"   = "bin-x86_64"
    "linux/arm64"   = "bin-aarch64"
    "darwin/amd64"  = "bin-macos-x86"
    "darwin/arm64"  = "bin-macos-arm64"
    "windows/amd64" = "bin-windows"
}

# List of commands to build
$Commands = @("mdcalc", "mdbackup", "mdrestore", "mdsource", "mdjournal", "mdlabel")

# Build for each target
foreach ($target in $Targets.Keys) {
    $GOOS, $GOARCH = $target -split "/"
    $OutputSubDir = $Targets[$target]
    $OutputDir = Join-Path $BaseBinDir $OutputSubDir
    
    Write-Host "Building for $GOOS/$GOARCH → $OutputDir"
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    
    foreach ($cmd in $Commands) {
        Write-Host "  - $cmd"
        $env:GOOS = $GOOS
        $env:GOARCH = $GOARCH
        $OutputPath = Join-Path $OutputDir $cmd
        if ($GOOS -eq "windows") {
            $OutputPath += ".exe"
        }
        & go build -o $OutputPath "./cmd/$cmd"
        if ($LASTEXITCODE -ne 0) {
            throw "Build failed for $cmd on $GOOS/$GOARCH"
        }
    }
}

Write-Host "Build complete!"
