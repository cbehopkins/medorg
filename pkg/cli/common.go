// Package cli provides common functionality for medorg command-line tools.
package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/cbehopkins/medorg/pkg/core"
)

// Common exit codes used across medorg tools
const (
	ExitOk                = 0
	ExitInvalidArgs       = 1
	ExitConfigError       = 2
	ExitNoSources         = 3
	ExitNoVolumeLabel     = 4
	ExitChecksumError     = 5
	ExitCollisionError    = 6
	ExitDiscoveryError    = 7
	ExitMetadataError     = 8
	ExitJournalNotFound   = 9
	ExitSourceNotFound    = 10
	ExitRestoreError      = 11
	ExitWalkError         = 12
	ExitJournalWriteError = 13
	ExitPathNotExist      = 14
	ExitAliasExists       = 15
	ExitAliasNotFound     = 16
	ExitRestoreSetError   = 17

	// Legacy exit codes from specific tools
	ExitNoConfig            = 2 // Alias for ExitConfigError
	ExitOneDirectoryOnly    = 20
	ExitTwoDirectoriesOnly  = 21
	ExitProgressBar         = 22
	ExitIncompleteBackup    = 23
	ExitSuppliedDirNotFound = 24
	ExitBadVc               = 25
	ExitNoSourcesConfigured = 26
)

// ConfigLoader handles loading and creating MdConfig with consistent error handling.
type ConfigLoader struct {
	ConfigPath string
	Stdout     io.Writer
}

// NewConfigLoader creates a new ConfigLoader with the given config path.
// If stdout is nil, it defaults to os.Stdout.
func NewConfigLoader(configPath string, stdout io.Writer) *ConfigLoader {
	if stdout == nil {
		stdout = os.Stdout
	}
	return &ConfigLoader{
		ConfigPath: configPath,
		Stdout:     stdout,
	}
}

// Load loads or creates the MdConfig.
// Returns the config and an exit code. Exit code is ExitOk on success.
func (cl *ConfigLoader) Load() (*core.MdConfig, int) {
	xc, err := core.LoadOrCreateMdConfigWithPath(cl.ConfigPath)
	if err != nil {
		fmt.Fprintf(cl.Stdout, "Error loading config file: %v\n", err)
		return nil, ExitConfigError
	}
	return xc, ExitOk
}

// MustLoad loads or creates the MdConfig and exits the program on error.
func (cl *ConfigLoader) MustLoad() *core.MdConfig {
	xc, exitCode := cl.Load()
	if exitCode != ExitOk {
		os.Exit(exitCode)
	}
	return xc
}

// SourceDirResolver handles resolving source directories from CLI args or config.
type SourceDirResolver struct {
	CLIArgs []string
	Config  *core.MdConfig
	Stdout  io.Writer
}

// NewSourceDirResolver creates a resolver for source directories.
func NewSourceDirResolver(cliArgs []string, config *core.MdConfig, stdout io.Writer) *SourceDirResolver {
	if stdout == nil {
		stdout = os.Stdout
	}
	return &SourceDirResolver{
		CLIArgs: cliArgs,
		Config:  config,
		Stdout:  stdout,
	}
}

// Resolve returns the list of source directories to use.
// Priority: CLI args > Config > Current directory
// Returns directories and an exit code (ExitOk on success).
func (sdr *SourceDirResolver) Resolve() ([]string, int) {
	var directories []string

	// Priority 1: Command line arguments
	if len(sdr.CLIArgs) > 0 {
		for _, dir := range sdr.CLIArgs {
			if isDir(dir) {
				directories = append(directories, dir)
			}
		}
		if len(directories) == 0 {
			fmt.Fprintln(sdr.Stdout, "Error: No valid directories found in arguments")
			return nil, ExitInvalidArgs
		}
		return directories, ExitOk
	}

	// Priority 2: Config file
	if sdr.Config != nil {
		directories = sdr.Config.GetSourcePaths()
		if len(directories) > 0 {
			return directories, ExitOk
		}
	}

	// Priority 3: Current directory
	return []string{"."}, ExitOk
}

// ResolveWithValidation resolves source directories and validates they exist.
// Returns directories and an exit code (ExitOk on success).
func (sdr *SourceDirResolver) ResolveWithValidation() ([]string, int) {
	var directories []string

	// Priority 1: Command line arguments
	if len(sdr.CLIArgs) > 0 {
		for _, dir := range sdr.CLIArgs {
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				fmt.Fprintf(sdr.Stdout, "Error: directory '%s' does not exist\n", dir)
				return nil, ExitSuppliedDirNotFound
			}
			if isDir(dir) {
				directories = append(directories, dir)
			}
		}
		return directories, ExitOk
	}

	// Priority 2: Config file
	if sdr.Config != nil {
		directories = sdr.Config.GetSourcePaths()
		if len(directories) > 0 {
			return directories, ExitOk
		}
		fmt.Fprintln(sdr.Stdout, "No source directories configured. Use 'mdsource add' to configure sources or provide directories as arguments.")
		return nil, ExitNoSourcesConfigured
	}

	// Priority 3: Current directory
	return []string{"."}, ExitOk
}

// isDir checks if a path is a directory
func isDir(fn string) bool {
	stat, err := os.Stat(fn)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

// ValidatePath checks if a path exists and optionally if it's a directory.
func ValidatePath(path string, mustBeDir bool) error {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("path '%s' does not exist", path)
	}
	if err != nil {
		return fmt.Errorf("error accessing path '%s': %w", path, err)
	}
	if mustBeDir && !stat.IsDir() {
		return fmt.Errorf("path '%s' is not a directory", path)
	}
	return nil
}

// ExitWithError prints an error message and exits with the given code.
func ExitWithError(exitCode int, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(exitCode)
}

// CommonConfig holds fields commonly used across tool configs
type CommonConfig struct {
	ConfigPath string
	Stdout     io.Writer
	XMLConfig  *core.MdConfig
	DryRun     bool
}

// SetupLogFile creates or opens a log file for writing and returns the file handle.
// The file is created in append mode. Caller is responsible for closing the file.
// Returns the file handle and an exit code (ExitOk on success, or an error code on failure).
func SetupLogFile(filename string) (io.Writer, int) {
	// Remove existing log file to start fresh
	os.Remove(filename)

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening log file %s: %v\n", filename, err)
		return nil, ExitConfigError
	}

	return f, ExitOk
}

// PrintError prints an error message to the writer and returns the exit code.
// This is useful for consistent error handling in Run() functions.
// If err is nil, nothing is printed.
func PrintError(err error, writer io.Writer) {
	if err != nil {
		fmt.Fprintf(writer, "Error: %v\n", err)
	}
}

// ExitFromRun standardizes the pattern for exiting from main() after calling Run().
// It prints any error to stderr and exits with the given exit code.
// This should be called from main() like: ExitFromRun(Run(cfg))
func ExitFromRun(exitCode int, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
	os.Exit(exitCode)
}
