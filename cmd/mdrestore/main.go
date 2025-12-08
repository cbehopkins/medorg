package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cbehopkins/medorg/pkg/cli"
)

func main() {
	cli.ExitFromRun(run())
}

func run() (int, error) {
	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.medorg.xml)")
	journalPath := flag.String("journal", "", "Path to journal file (required)")
	flag.Parse()

	if *journalPath == "" {
		printUsage()
		return cli.ExitInvalidArgs, nil
	}

	if flag.NArg() < 1 {
		fmt.Println("Error: source directory (backup location) required")
		printUsage()
		return cli.ExitInvalidArgs, nil
	}

	sourceDir := flag.Arg(0)

	// Verify journal exists
	if err := cli.ValidatePath(*journalPath, false); err != nil {
		fmt.Printf("Error: journal file '%s' does not exist\n", *journalPath)
		return cli.ExitJournalNotFound, nil
	}

	// Verify source directory exists
	if err := cli.ValidatePath(sourceDir, true); err != nil {
		fmt.Printf("Error: source directory '%s' does not exist\n", sourceDir)
		return cli.ExitSourceNotFound, nil
	}

	// Load config using common loader
	loader := cli.NewConfigLoader(*configPath, os.Stderr)
	xc, exitCode := loader.Load()
	if exitCode != cli.ExitOk {
		return exitCode, nil
	}

	// Run restore
	cfg := Config{
		JournalPath: *journalPath,
		SourceDir:   sourceDir,
		XMLConfig:   xc,
		Stdout:      os.Stdout,
	}

	return Run(cfg)
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
