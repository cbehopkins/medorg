package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	core "github.com/cbehopkins/medorg/pkg/core"
)

var scanSourceForPendingFunc = consumers.ScanSourceForPending
var executeCopyOperationFunc = executeCopy

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

	var insertedCount int
	var failedCount int
	var pendingCount int

	err = consumers.WithRestoreDB(*dbPath, func(db *consumers.RestoreDB) error {
		var innerErr error
		insertedCount, failedCount, innerErr = parseJournalAndInsert(bytes.NewReader(journalContent), db, xc)
		if innerErr != nil {
			return fmt.Errorf("failed to parse journal: %w", innerErr)
		}

		pendingCount, innerErr = db.CountPending()
		if innerErr != nil {
			return fmt.Errorf("failed to count pending tasks: %w", innerErr)
		}

		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return cli.ExitRestoreError, nil
	}

	fmt.Printf("Journal loaded from %s (%d bytes)\n", *journalPath, len(journalContent))
	fmt.Printf("Config loaded with %d restore destinations\n", len(xc.RestoreDestinations))
	fmt.Printf("Ingested %d restore targets, %d failed\n", insertedCount, failedCount)

	fmt.Printf("Database state: %d pending content nodes\n", pendingCount)

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

	var plan *consumers.CopyPlan
	var copiedCount int
	var errorCount int
	var pendingByVol map[string]int

	if err := consumers.WithRestoreDB(*dbPath, func(db *consumers.RestoreDB) error {
		var innerErr error
		// Scan source directory for pending content
		plan, innerErr = buildCopyPlan(sourceDir, db)
		if innerErr != nil {
			return fmt.Errorf("failed to scan source: %w", innerErr)
		}

		if *dryRun {
			return nil
		}

		// Execute copy operations
		for _, op := range plan.Operations {
			if innerErr = executeCopyOperationFunc(&op); innerErr != nil {
				fmt.Printf("Error copying %s: %v\n", op.SourcePath, innerErr)
				errorCount++
				continue
			}

			// Move target from pending to copied
			if innerErr = db.MoveToCopied(op.DestinationPath, op.MD5, op.Size); innerErr != nil {
				fmt.Printf("Error updating database for %s: %v\n", op.DestinationPath, innerErr)
				errorCount++
				continue
			}

			copiedCount++
		}

		pendingByVol, _ = db.CountPendingByBackupDest()
		return nil
	}); err != nil {
		fmt.Printf("Error: %v\n", err)
		return cli.ExitRestoreError, nil
	}

	fmt.Printf("Scan complete: %d matched, %d unmatched, %d errors\n",
		plan.MatchedCount, plan.UnmatchedCount, plan.ErrorCount)
	fmt.Printf("Planned %d copy operations\n", len(plan.Operations))

	if *dryRun {
		fmt.Println("\n[DRY RUN] Would copy:")
		for _, op := range plan.Operations {
			fmt.Printf("  %s -> %s (%s)\n", op.SourcePath, op.DestinationPath, op.Alias)
		}
		return cli.ExitOk, nil
	}

	fmt.Printf("\nCopy results: %d successful, %d errors\n", copiedCount, errorCount)

	// Show remaining pending files grouped by backup volume
	if len(pendingByVol) > 0 {
		fmt.Println("\n=== Remaining pending files by backup volume ===")
		fmt.Println("Volume          Files Remaining")
		fmt.Println("--------------- ---------------")
		totalRemaining := 0
		for vol, count := range pendingByVol {
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

	if errorCount > 0 {
		return cli.ExitRestoreError, nil
	}

	return cli.ExitOk, nil
}

func buildCopyPlan(sourceDir string, db *consumers.RestoreDB) (*consumers.CopyPlan, error) {
	return scanSourceForPendingFunc(sourceDir, db)
}

// executeCopy performs a single file copy operation.
func executeCopy(op *consumers.CopyOperation) error {
	// Ensure destination directory exists
	destDir := filepath.Dir(op.DestinationPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Copy file
	data, err := os.ReadFile(op.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source: %w", err)
	}

	if err := os.WriteFile(op.DestinationPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write destination: %w", err)
	}

	return nil
}

// parseJournalAndInsert reads journal XML and inserts restore targets into the database.
// It maps journal aliases to restore destinations from the config and creates RestoreTaskTarget entries.
// Returns (insertedCount, failedCount, error).
func parseJournalAndInsert(r io.Reader, db *consumers.RestoreDB, xc *core.MdConfig) (int, int, error) {
	insertedCount := 0
	failedCount := 0

	decoder := xml.NewDecoder(r)

	// We need to parse the XML stream manually to handle the mdj/dr/fr structure
	// The format is: <mdj alias="..."><dr dir="..."><fr fname="..." checksum="..." size="..." mtime="..." />...</dr>...</mdj>

	var currentAlias string
	var currentDir string

	for {
		token, err := decoder.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return insertedCount, failedCount, fmt.Errorf("error reading journal XML: %w", err)
		}

		// Handle start elements
		startElem, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		switch startElem.Name.Local {
		case "mdj":
			// Extract alias from <mdj alias="...">
			for _, attr := range startElem.Attr {
				if attr.Name.Local == "alias" {
					currentAlias = attr.Value
					break
				}
			}

		case "dr":
			// Extract dir from <dr dir="...">
			for _, attr := range startElem.Attr {
				if attr.Name.Local == "dir" {
					currentDir = attr.Value
					break
				}
			}

		case "fr":
			// Parse file entry: <fr fname="..." checksum="..." size="..." mtime="..." bd="..."/>
			if currentAlias == "" {
				failedCount++
				continue
			}

			var fileName string
			var checksum string
			var size int64
			var backupDests []string

			for _, attr := range startElem.Attr {
				switch attr.Name.Local {
				case "fname":
					fileName = attr.Value
				case "checksum":
					checksum = attr.Value
				case "size":
					fmt.Sscanf(attr.Value, "%d", &size)
				case "bd":
					// BackupDest is stored as a single value in file entries
					// For files with multiple backup destinations, they're duplicated
					// or stored separately. For now, handle single value.
					backupDests = append(backupDests, attr.Value)
				}
			}

			// Create restore target path based on alias, dir, and filename
			targetAbsPath := filepath.Join("/restore", currentAlias, filepath.Base(currentDir), fileName)
			taskID := fmt.Sprintf("%s:%d:%s", checksum, size, targetAbsPath)

			target := &consumers.RestoreTaskTarget{
				TaskID:        taskID,
				Alias:         currentAlias,
				TargetAbsPath: targetAbsPath,
				CreatedAtUnix: time.Now().Unix(),
			}

			// Insert into database
			if err := db.InsertPending(target, checksum, size, backupDests); err != nil {
				fmt.Printf("Warning: failed to insert %s: %v\n", fileName, err)
				failedCount++
				continue
			}

			insertedCount++
		}
	}

	return insertedCount, failedCount, nil
}
