package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
)

// newdbSubcommand implements the "newdb" phase: load journal into database.
func newdbSubcommand(args []string) (int, error) {
	fs := flag.NewFlagSet("newdb", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to config file (optional, defaults to ~/.mdcfg.xml)")
	journalPath := fs.String("journal", "", "Path to journal XML file (required)")
	dbPath := fs.String("db", "restore.db", "Path to restore database (default: restore.db)")
	fs.Parse(args)

	if *journalPath == "" {
		fmt.Println("Error: --journal is required")
		return cli.ExitInvalidArgs, nil
	}

	// Validate paths
	if err := cli.ValidatePath(*journalPath, false); err != nil {
		fmt.Printf("Error: journal file '%s' does not exist\n", *journalPath)
		return cli.ExitJournalNotFound, nil
	}

	// Load config
	loader := cli.NewConfigLoader(*configPath, os.Stderr)
	xc, exitCode := loader.Load()
	if exitCode != cli.ExitOk {
		return exitCode, nil
	}

	// Parse journal file
	journalContent, err := os.ReadFile(*journalPath)
	if err != nil {
		fmt.Printf("Error: failed to read journal file: %v\n", err)
		return cli.ExitJournalNotFound, nil
	}

	var ingestResult consumers.JournalIngestResult

	err = consumers.WithRestoreContext(*dbPath, func(restore *consumers.RestoreContext) error {
		var innerErr error
		ingestResult, innerErr = restore.IngestJournal(bytes.NewReader(journalContent), xc)
		if innerErr != nil {
			return fmt.Errorf("failed to parse journal: %w", innerErr)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return cli.ExitRestoreError, nil
	}

	for _, warning := range ingestResult.Warnings {
		fmt.Printf("Warning: failed to insert %s: %v\n", warning.FileName, warning.Err)
	}
	fmt.Printf("Journal loaded from %s (%d bytes)\n", *journalPath, len(journalContent))
	fmt.Printf("Config loaded with %d restore destinations\n", len(xc.RestoreDestinations))
	fmt.Printf("Ingested %d restore targets, %d failed\n", ingestResult.InsertedCount, ingestResult.FailedCount)

	fmt.Printf("Database state: %d pending content nodes\n", ingestResult.PendingCount)

	return cli.ExitOk, nil
}

// copySubcommand implements the "copy" phase: scan source and execute copies.
func copySubcommand(args []string) (int, error) {
	fs := flag.NewFlagSet("copy", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to config file (optional)")
	dbPath := fs.String("db", "restore.db", "Path to restore database (default: restore.db)")
	dryRun := fs.Bool("dry-run", false, "Show what would be copied without actually copying")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Println("Error: source directory (backup location) required")
		return cli.ExitInvalidArgs, nil
	}

	sourceDir := fs.Arg(0)

	// Validate source directory
	if err := cli.ValidatePath(sourceDir, true); err != nil {
		fmt.Printf("Error: source directory '%s' does not exist\n", sourceDir)
		return cli.ExitSourceNotFound, nil
	}

	// Load config
	loader := cli.NewConfigLoader(*configPath, os.Stderr)
	xc, exitCode := loader.Load()
	if exitCode != cli.ExitOk {
		return exitCode, nil
	}
	_ = xc // Config loaded for future use (e.g., restore destination mapping)

	var result *consumers.CopyResult

	if err := consumers.WithRestoreContext(*dbPath, func(restore *consumers.RestoreContext) error {
		var innerErr error
		result, innerErr = restore.CopyFrom(sourceDir, *dryRun)
		return innerErr
	}); err != nil {
		fmt.Printf("Error: %v\n", err)
		return cli.ExitRestoreError, nil
	}
	for _, failure := range result.Failures {
		if failure.StateUpdate {
			fmt.Printf("Error updating database for %s: %v\n", failure.Operation.DestinationPath, failure.Err)
			continue
		}
		fmt.Printf("Error copying %s: %v\n", failure.Operation.SourcePath, failure.Err)
	}
	plan := result.Plan

	fmt.Printf("Scan complete: %d matched, %d unmatched, %d errors\n",
		plan.MatchedCount, plan.UnmatchedCount, plan.ErrorCount)
	fmt.Printf("Planned %d copy operations\n", len(plan.Operations))

	if *dryRun {
		fmt.Printf("\n[DRY RUN] Already correct: %d\n", result.AlreadyPresentCount)
		fmt.Println("Would copy:")
		for _, op := range plan.Operations {
			fmt.Printf("  %s -> %s (%s)\n", op.SourcePath, op.DestinationPath, op.Alias)
		}
		return cli.ExitOk, nil
	}

	fmt.Printf("\nCopy results: %d copied, %d already correct, %d errors\n",
		result.CopiedCount, result.AlreadyPresentCount, len(result.Failures))

	// Show remaining pending files grouped by backup volume
	if len(result.PendingByBackupDest) > 0 {
		fmt.Println("\n=== Remaining pending files by backup volume ===")
		fmt.Println("Volume          Files Remaining")
		fmt.Println("--------------- ---------------")
		totalRemaining := 0
		for vol, count := range result.PendingByBackupDest {
			fmt.Printf("%-15s %d\n", vol, count)
			totalRemaining += count
		}
		fmt.Println("--------------- ---------------")
		fmt.Printf("%-15s %d\n", "TOTAL", totalRemaining)
		fmt.Println()
		if totalRemaining > 0 {
			fmt.Println("Next steps:")
			fmt.Println("  1. Mount the next backup volume")
			fmt.Println("  2. Run: mdrestore copy --db <database> <new_source_directory>")
		}
	}

	if len(result.Failures) > 0 {
		return cli.ExitRestoreError, nil
	}

	return cli.ExitOk, nil
}
