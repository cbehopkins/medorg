package main

import (
	"fmt"
	"io"
	"os"

	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Config holds the configuration for mdjournal
type Config struct {
	Directories  []string
	JournalPath  string
	ScanOnly     bool
	Stdout       io.Writer
	ReadExisting bool
}

// Run is the main logic, extracted for testability
// Returns exit code and error
func Run(cfg Config) (int, error) {
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}

	if cfg.ScanOnly {
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
		return ExitWalkError, fmt.Errorf("error running check_calc: %w", err)
	}

	journal := consumers.Journal{}

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

	// Step 2: Read .medorg.xml files and populate journal
	// No checksum calculation needed - just read the existing data
	for _, dir := range cfg.Directories {
		dm, err := core.DirectoryMapFromDir(dir)
		if err != nil {
			return ExitWalkError, fmt.Errorf("error reading directory map from %s: %w", dir, err)
		}

		// Add the directory map to the journal
		if err := journal.AppendJournalFromDm(&dm, dir); err != nil {
			// ErrFileExistsInJournal is not a real error, just informational
			if err != consumers.ErrFileExistsInJournal {
				return ExitWalkError, fmt.Errorf("error adding directory to journal: %w", err)
			}
		}
	}

	// Write journal to file
	fh, err := os.Create(cfg.JournalPath)
	if err != nil {
		return ExitJournalWriteError, fmt.Errorf("unable to open journal for writing: %w", err)
	}
	defer fh.Close()

	if err := journal.ToWriter(fh); err != nil {
		return ExitJournalWriteError, fmt.Errorf("error writing journal: %w", err)
	}

	return ExitOk, nil
}
