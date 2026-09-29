package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

type scanPathsFlag []string

func (s *scanPathsFlag) String() string {
	return fmt.Sprint([]string(*s))
}

func (s *scanPathsFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func resolveDirectoriesForRun(scanDirs []string, cliArgs []string, xc *core.MdConfig, stdout io.Writer) ([]string, int) {
	if len(scanDirs) > 0 {
		for _, dir := range scanDirs {
			if xc == nil || xc.GetAliasForPath(dir) == "" {
				fmt.Fprintf(stdout, "Error: scan path '%s' is not configured in %s\n", dir, core.ConfigFileName)
				return nil, cli.ExitConfigError
			}
		}
		return scanDirs, cli.ExitOk
	}

	resolver := cli.NewSourceDirResolver(cliArgs, xc, stdout)
	return resolver.ResolveWithValidation()
}

func main() {
	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.mdcfg.xml)")
	outputPath := flag.String("output", "", "Path to output journal file (optional, defaults to .mdjournal.xml)")
	var scanDirs scanPathsFlag
	flag.Var(&scanDirs, "scan", "Directory to scan (repeat for multiple directories); must be configured in .mdcfg.xml")

	flag.Parse()

	// Load config using common loader
	loader := cli.NewConfigLoader(*configPath, os.Stderr)
	xc, exitCode := loader.Load()
	if exitCode != cli.ExitOk {
		os.Exit(exitCode)
	}

	// Resolve source directories from --scan values or args/config fallback.
	directories, exitCode := resolveDirectoriesForRun(scanDirs, flag.Args(), xc, os.Stdout)
	if exitCode != cli.ExitOk {
		os.Exit(exitCode)
	}

	// Create alias lookup function if config available
	var getAlias func(string) string
	var ignorePatterns []string
	if xc != nil {
		getAlias = xc.GetAliasForPath
		ignorePatterns = xc.IgnorePatterns
	}

	journalPath := string(core.ConfigPath(core.JournalPathName))
	if *outputPath != "" {
		journalPath = *outputPath
	}

	cfg := Config{
		Directories:    directories,
		JournalPath:    journalPath,
		ScanOnly:       len(scanDirs) > 0,
		ReadExisting:   false, // Always start fresh
		IgnorePatterns: ignorePatterns,
		GetAlias:       getAlias,
	}

	cli.ExitFromRun(Run(cfg))
}
