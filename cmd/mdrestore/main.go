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
	if len(os.Args) < 2 {
		printUsage()
		return cli.ExitInvalidArgs, nil
	}

	subcommand := os.Args[1]
	subArgs := os.Args[2:]

	switch subcommand {
	case "newdb":
		return newdbSubcommand(subArgs)
	case "copy":
		return copySubcommand(subArgs)
	case "help", "-h", "--help":
		printUsage()
		return cli.ExitOk, nil
	default:
		fmt.Printf("Error: unknown subcommand '%s'\n", subcommand)
		printUsage()
		return cli.ExitInvalidArgs, nil
	}
}

// legacyRun preserves original single-phase behavior for backward compatibility
func legacyRun() (int, error) {
	configPath := flag.String("config", "", "Path to config file (optional, defaults to ~/.mdcfg.xml)")
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
	fmt.Println("mdrestore - Two-phase restore from backup using journal and database")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  mdrestore <subcommand> [options...]")
	fmt.Println("")
	fmt.Println("Subcommands:")
	fmt.Println("  newdb   Load journal into restore database (phase 1)")
	fmt.Println("  copy    Scan source and copy matched files (phase 2)")
	fmt.Println("")
	fmt.Println("newdb options:")
	fmt.Println("  --config <path>   Path to config file (optional)")
	fmt.Println("  --journal <path>  Path to journal XML file (required)")
	fmt.Println("  --db <path>       Path to restore database (default: restore.db)")
	fmt.Println("")
	fmt.Println("copy options:")
	fmt.Println("  --config <path>   Path to config file (optional)")
	fmt.Println("  --db <path>       Path to restore database (default: restore.db)")
	fmt.Println("  --dry-run         Show what would be copied without copying")
	fmt.Println("  <source-dir>      Backup volume directory to scan (required)")
	fmt.Println("")
	fmt.Println("Workflow:")
	fmt.Println("  1. mdrestore newdb --journal backup.journal")
	fmt.Println("  2. mdrestore copy /mnt/backup1")
	fmt.Println("  3. mdrestore copy /mnt/backup2 (repeat for additional volumes)")
	fmt.Println("")
	fmt.Println("Example:")
	fmt.Println("  mdrestore newdb --journal backup.journal --db restore.db")
	fmt.Println("  mdrestore copy --dry-run /mnt/backup1")
	fmt.Println("  mdrestore copy /mnt/backup1")
}
