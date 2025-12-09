package main

import (
	"fmt"
	"io"
	"os"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
)

// Config holds the configuration for mdjournal
type Config struct {
	Directories  []string
	JournalPath  string
	ScanOnly     bool
	Stdout       io.Writer
	ReadExisting bool
	// GetAlias returns the alias for a given path, or empty string if not found
	GetAlias func(path string) string
}

// Run is the main logic, extracted for testability
// Returns exit code and error
func Run(cfg Config) (int, error) {
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}

	if cfg.ScanOnly {
		// FIXME this flag does nothing
		// It should not write to the journal file, but currently it does
		fmt.Fprintln(cfg.Stdout, "You've asked us to scan:", cfg.Directories)
	}

	// Step 1: Run check_calc to generate/update .medorg.xml files
	// This calculates MD5 checksums for all files in all directories
	checkCalcOpts := consumers.CheckCalcOptions{
		CalcCount: 2, // Default parallelism
		Recalc:    false,
		Validate:  false,
		Scrub:     false,
		AutoFix:   nil,
	}
	if err := consumers.RunCheckCalc(cfg.Directories, checkCalcOpts); err != nil {
		return cli.ExitWalkError, fmt.Errorf("error running check_calc: %w", err)
	}

	journal := consumers.NewJournal()

	// Read existing journal if requested and file exists
	if cfg.ReadExisting {
		fh, err := os.Open(cfg.JournalPath)
		if err == nil {
			fmt.Fprintln(cfg.Stdout, "Reading in journal")
			if err := journal.FromReader(fh); err != nil {
				fmt.Fprintln(cfg.Stdout, "Error reading journal:", err)
			}
			if err := fh.Close(); err != nil {
				fmt.Fprintln(cfg.Stdout, "Error closing read in journal:", err)
			}
		} else if !os.IsNotExist(err) {
			fmt.Fprintln(cfg.Stdout, "Error opening journal:", err)
		}
	}
	fmt.Println("Using Journal Path:", cfg.JournalPath)

	// Step 2: Populate journal from directories with recursive traversal
	// Each directory gets the appropriate alias and all subdirectories are included
	for _, dir := range cfg.Directories {
		// Get alias for this directory if available
		var alias string
		if cfg.GetAlias != nil {
			// FIXME this is a messy design - but works for now
			alias = cfg.GetAlias(dir)
		}
		if alias == "" {
			// Alias is required for journal entries
			return cli.ExitAliasNotFound, fmt.Errorf("alias not provided for journal: %s", dir)
		}

		// Populate journal using PopulateFromDirectories which handles recursion
		if err := journal.PopulateFromDirectories(dir, alias); err != nil {
			return cli.ExitWalkError, fmt.Errorf("error populating journal from %s: %w", dir, err)
		}
	}

	// Write journal to file
	fh, err := os.Create(cfg.JournalPath)
	if err != nil {
		return cli.ExitJournalWriteError, fmt.Errorf("unable to open journal for writing: %w", err)
	}
	defer fh.Close()

	if err := journal.ToWriter(fh); err != nil {
		return cli.ExitJournalWriteError, fmt.Errorf("error writing journal: %w", err)
	}
	fmt.Fprintln(cfg.Stdout, "Journal written to", cfg.JournalPath)

	return cli.ExitOk, nil
}
