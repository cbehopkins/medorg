package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cbehopkins/medorg/pkg/core"
)

const (
	ExitOk = iota
	ExitNoConfig
	ExitInvalidArgs
	ExitJournalNotFound
	ExitSourceNotFound
	ExitRestoreError
)

func main() {
	retcode := 0
	defer func() { os.Exit(retcode) }()

	// Command line flags
	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.medorg.xml)")
	journalPath := flag.String("journal", "", "Path to journal file (required)")
	flag.Parse()

	if *journalPath == "" {
		printUsage()
		retcode = ExitInvalidArgs
		return
	}

	if flag.NArg() < 1 {
		fmt.Println("Error: source directory (backup location) required")
		printUsage()
		retcode = ExitInvalidArgs
		return
	}

	sourceDir := flag.Arg(0)

	// Verify journal exists
	if _, err := os.Stat(*journalPath); os.IsNotExist(err) {
		fmt.Printf("Error: journal file '%s' does not exist\n", *journalPath)
		retcode = ExitJournalNotFound
		return
	}

	// Verify source directory exists
	if _, err := os.Stat(sourceDir); os.IsNotExist(err) {
		fmt.Printf("Error: source directory '%s' does not exist\n", sourceDir)
		retcode = ExitSourceNotFound
		return
	}

	// Load XMLCfg
	xc, err := core.LoadOrCreateMdConfigWithPath(*configPath)
	if err != nil {
		fmt.Println("Error loading config file:", err)
		retcode = ExitNoConfig
		return
	}

	// Run restore
	cfg := Config{
		JournalPath: *journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      os.Stdout,
	}

	exitCode, err := Run(cfg)
	if err != nil {
		fmt.Println("Error:", err)
	}
	retcode = exitCode
}

func printUsage() {
	fmt.Println("mdrestore - Restore files from backup using journal")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  mdrestore --journal <journal-file> <source-directory>")
	fmt.Println("")
	fmt.Println("Arguments:")
	fmt.Println("  source-directory  The backup location to restore from")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("  --config <path>   Path to config file (optional, defaults to ~/.medorg.xml)")
	fmt.Println("  --journal <path>  Path to the journal file (required)")
	fmt.Println("")
	fmt.Println("Before restoring, configure restore destinations:")
	fmt.Println("  mdsource restore -alias media -path /restore/to/here")
	fmt.Println("")
	fmt.Println("Example:")
	fmt.Println("  mdrestore --journal backup.journal /mnt/backup1")
}
