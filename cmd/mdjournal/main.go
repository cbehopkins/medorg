package main

import (
	"flag"
	"os"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.medorg.xml)")
	scanflg := flag.Bool("scan", false, "Only scan files in src & dst updating labels, don't run the backup")

	flag.Parse()

	// Load config using common loader
	loader := cli.NewConfigLoader(*configPath, os.Stderr)
	xc, exitCode := loader.Load()
	if exitCode != cli.ExitOk {
		os.Exit(exitCode)
	}

	// Resolve source directories using common resolver
	resolver := cli.NewSourceDirResolver(flag.Args(), xc, os.Stdout)
	directories, exitCode := resolver.ResolveWithValidation()
	if exitCode != cli.ExitOk {
		os.Exit(exitCode)
	}

	// Create alias lookup function if config available
	var getAlias func(string) string
	if xc != nil {
		getAlias = xc.GetAliasForPath
	}

	cfg := Config{
		Directories:  directories,
		JournalPath:  string(core.ConfigPath(core.JournalPathName)),
		ScanOnly:     *scanflg,
		ReadExisting: true,
		GetAlias:     getAlias,
	}

	cli.ExitFromRun(Run(cfg))
}
