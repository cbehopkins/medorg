# CLI Package - Code Deduplication Guide

## Overview

The `pkg/cli` package provides common functionality for medorg command-line tools, eliminating code duplication across the various `cmd/` packages.

## What Was Created

### 1. Common Exit Codes (`pkg/cli/common.go`)

All tools now share standardized exit codes:

```go
const (
    ExitOk               = 0
    ExitInvalidArgs      = 1
    ExitConfigError      = 2
    ExitNoSources        = 3
    ExitNoVolumeLabel    = 4
    ExitChecksumError    = 5
    ExitCollisionError   = 6
    ExitDiscoveryError   = 7
    ExitMetadataError    = 8
    ExitJournalNotFound  = 9
    ExitSourceNotFound   = 10
    ExitRestoreError     = 11
    // ... and more
)
```

**Benefits:**
- Consistent exit codes across all tools
- Single source of truth for error codes
- Easier to document and understand tool behavior

### 2. ConfigLoader Helper

Standardizes config file loading:

```go
loader := cli.NewConfigLoader(configPath, os.Stderr)
xc, exitCode := loader.Load()
if exitCode != cli.ExitOk {
    os.Exit(exitCode)
}
```

**Replaces:**
```go
// Old way (repeated in every tool):
xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
if err != nil {
    fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
    os.Exit(ExitConfigError)
}
```

### 3. SourceDirResolver Helper

Handles the common pattern of getting directories from CLI args or config:

```go
resolver := cli.NewSourceDirResolver(flag.Args(), xc, os.Stdout)
directories, exitCode := resolver.Resolve()
if exitCode != cli.ExitOk {
    os.Exit(exitCode)
}
```

**Features:**
- Priority: CLI args > Config > Current directory
- `Resolve()` - basic resolution
- `ResolveWithValidation()` - validates paths exist

### 4. Path Validation

Common path validation helper:

```go
if err := cli.ValidatePath(path, true); err != nil {
    fmt.Fprintf(os.Stderr, "Error: %v\n", err)
    os.Exit(cli.ExitPathNotExist)
}
```

### 5. CommonConfig Struct

Base config structure that can be embedded:

```go
type CommonConfig struct {
    ConfigPath string
    Stdout     io.Writer
    XMLConfig  *core.MdConfig
    DryRun     bool
}

// Tool-specific config can embed it:
type Config struct {
    cli.CommonConfig
    DestinationDir string
    SourceDirs     []string
}
```

### 6. SetupLogFile Helper

Simplifies log file initialization for tools that need logging:

```go
f, exitCode := cli.SetupLogFile("mdbackup.log")
if exitCode != cli.ExitOk {
    fmt.Printf("error opening log file: %v\n", exitCode)
    os.Exit(exitCode)
}
defer f.(*os.File).Close()

log.SetOutput(f)
```

**Replaces:**
```go
// Old way (repeated in mdbackup):
os.Remove(LOGFILENAME)
f, err := os.OpenFile(LOGFILENAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
if err != nil {
    fmt.Printf("error opening log file: %v\n", err)
    os.Exit(1)
}
defer f.Close()
```

### 7. PrintError Helper

Consistent error printing for Run() functions:

```go
exitCode, err := Run(cfg)
cli.PrintError(err, os.Stderr)
os.Exit(exitCode)
```

**Features:**
- Only prints if error is not nil
- Consistent format across all tools
- Reduces boilerplate in main functions

### 8. ExitFromRun Helper

Standardized main() exit pattern for tools with extracted Run() functions:

```go
func main() {
	cli.ExitFromRun(Run(cfg))
}
```

**Replaces:**
```go
// Old way (3 lines of boilerplate):
exitCode, err := Run(cfg)
if err != nil {
	fmt.Fprintln(os.Stderr, err)
}
os.Exit(exitCode)
```

**Standardizes:**
- All tools with `Run(cfg Config) (int, error)` can use this pattern
- Error automatically printed to stderr
- Clean, one-line call in main()
- Consistent exit behavior across all tools

## Migration Example: mddiscover

### Before:
```go
import (
    "github.com/cbehopkins/medorg/pkg/core"
)

const (
    ExitOk             = 0
    ExitInvalidArgs    = 1
    ExitConfigError    = 2
    // ... repeated in every tool
)

func main() {
    // ... flag parsing ...
    
    xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
        os.Exit(ExitConfigError)
    }
    
    // ... rest of code uses ExitOk, ExitInvalidArgs, etc.
}
```

### After:
```go
import (
    "github.com/cbehopkins/medorg/pkg/cli"
    "github.com/cbehopkins/medorg/pkg/core"
)

// Exit codes removed - use cli.ExitOk, cli.ExitInvalidArgs, etc.

func main() {
    // ... flag parsing ...
    
    loader := cli.NewConfigLoader(configPath, os.Stderr)
    xc, exitCode := loader.Load()
    if exitCode != cli.ExitOk {
        os.Exit(exitCode)
    }
    
    // ... rest of code uses cli.ExitOk, cli.ExitInvalidArgs, etc.
}
```

## How to Migrate Other Tools

### Step 1: Add cli import
```go
import (
    "github.com/cbehopkins/medorg/pkg/cli"
    // ... other imports
)
```

### Step 2: Remove local exit code constants
Delete the `const` block with exit codes from the tool's main.go.

### Step 3: Replace exit code references
Use find/replace to update all references:
- `ExitOk` → `cli.ExitOk`
- `ExitInvalidArgs` → `cli.ExitInvalidArgs`
- `ExitConfigError` → `cli.ExitConfigError`
- etc.

PowerShell command for bulk replacement:
```powershell
(Get-Content cmd\TOOLNAME\main.go) -replace '\bExitOk\b', 'cli.ExitOk' | Set-Content cmd\TOOLNAME\main.go
```

### Step 4: Use ConfigLoader
Replace:
```go
xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
if err != nil {
    fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
    os.Exit(ExitConfigError)
}
```

With:
```go
loader := cli.NewConfigLoader(configPath, os.Stderr)
xc, exitCode := loader.Load()
if exitCode != cli.ExitOk {
    os.Exit(exitCode)
}
```

### Step 5: (Optional) Use SourceDirResolver
If your tool resolves source directories from CLI args or config, replace:

```go
var directories []string
if flag.NArg() > 0 {
    for _, fl := range flag.Args() {
        if isDir(fl) {
            directories = append(directories, fl)
        }
    }
} else if xc != nil {
    directories = xc.GetSourcePaths()
    if len(directories) == 0 {
        directories = []string{"."}
    }
} else {
    directories = []string{"."}
}
```

With:
```go
resolver := cli.NewSourceDirResolver(flag.Args(), xc, os.Stdout)
directories, exitCode := resolver.Resolve()
if exitCode != cli.ExitOk {
    os.Exit(exitCode)
}
```

### Step 6: Update tests
Don't forget to:
1. Add `"github.com/cbehopkins/medorg/pkg/cli"` import to test files
2. Replace exit code references in test assertions

## Tools to Migrate

Priority order based on code duplication:

1. ✅ **mddiscover** - COMPLETED (example implementation)
2. **mdjournal** - Uses ExitOk, ExitNoConfig, ExitInvalidArgs, etc.
3. **mdcalc** - Has isDir() helper, config loading, source dir resolution
4. **mdbackup** - Has exit codes, isDir() helper
5. **mdrestore** - Has exit codes, config loading, path validation
6. **mdsource** - Has exit codes, uses subcommands (different pattern)
7. **mdlabel** - Simpler, less duplication (lowest priority)

## Testing

After migration, verify:
1. Tool builds: `go build .\cmd\TOOLNAME\`
2. Tests pass: `go test .\cmd\TOOLNAME\ -v`
3. Tool still works: Run basic commands manually

## Run() Function Standardization

All tools that extract business logic into a separate Run() function should follow this pattern:

### Signature
```go
type Config struct {
	// Tool-specific configuration fields
	ConfigPath string
	Stdout     io.Writer
	XMLConfig  *core.MdConfig
	// ... other fields specific to the tool
}

// Run executes the main logic
// Returns exit code and error (error should contain the error message)
func Run(cfg Config) (int, error) {
	// Business logic here
	// Return (cli.ExitOk, nil) on success
	// Return (cli.ExitXxx, fmt.Errorf(...)) on failure
}
```

### Usage in main()
```go
func main() {
	cli.ExitFromRun(Run(cfg))
}
```

### Benefits
- Testable: Run() function can be tested independently of CLI
- Reusable: Run() can be called from multiple entry points if needed
- Consistent: All tools follow the same pattern
- Clean: main() is just setup + one call to ExitFromRun()

### Current Tools Using This Pattern
- ✅ mddiscover: `Run(cfg Config) (int, error)`
- ✅ mdjournal: `Run(cfg Config) (int, error)`
- ✅ mdbackup: `Run(cfg Config) (int, error)`
- ✅ mdrestore: `Run(cfg Config) (int, error)` + helper run() function
- ✅ mdlabel: `run() (int, error)` called from main()
- ⏸️ mdsource: Uses custom run() function (subcommand pattern - OK as-is)
- ⏸️ mdcalc: Calls consumers.RunCheckCalc directly (OK as-is - is a wrapper tool)

**Lines of Code Saved:**
- ~10-15 lines per tool for exit codes
- ~5-8 lines per tool for config loading
- ~15-25 lines per tool for source directory resolution
- ~8-12 lines per tool for logging setup (where applicable)
- ~2-4 lines per tool for error printing
- **Total: ~40-64 lines per tool, ~280-450 lines across all tools**

**Maintenance Benefits:**
- Single place to add new exit codes
- Consistent error handling patterns
- Easier to understand and modify
- Better testability (helpers have unit tests)

**Developer Experience:**
- New tools can copy boilerplate from working examples
- Less repetitive code to write
- Clear patterns to follow
