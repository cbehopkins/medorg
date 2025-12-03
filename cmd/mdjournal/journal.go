package main

import (
	"fmt"
	"io"
	"io/fs"
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

	journal := consumers.Journal{}
	// Store directory maps to add to journal after processing
	dirMaps := make(map[string]*core.DirectoryMap)

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

	// Visitor function - update directory map with file information
	visitor := func(dm core.DirectoryMap, directory, file string, d fs.DirEntry) error {
		// Skip the md5 file itself
		if file == core.Md5FileName {
			return nil
		}

		// Skip hidden files (files starting with .)
		if len(file) > 0 && file[0] == '.' {
			return nil
		}

		// Update the directory map with this file's information
		fc := func(fs *core.FileStruct) error {
			info, err := d.Info()
			if err != nil {
				return err
			}
			_, err = fs.FromStat(directory, file, info)
			if err != nil {
				return err
			}
			// Update checksum for the file
			return fs.UpdateChecksum(false)
		}
		return dm.RunFsFc(directory, file, fc)
	}

	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			if err != nil {
				return &dm, err
			}
			dm.VisitFunc = visitor
			// Store the directory map pointer so we can add to journal later
			dirMaps[dir] = &dm
			return &dm, nil
		}
		de, err := core.NewDirectoryEntry(dir, mkFk)
		return de, err
	}

	// Walk directories and populate journal
	for _, dir := range cfg.Directories {
		errChan := core.NewDirTracker(false, dir, makerFunc).ErrChan()
		for err := range errChan {
			return ExitWalkError, fmt.Errorf("error walking %s: %w", dir, err)
		}
	}

	// Now add all the processed directory maps to the journal
	for dir, dm := range dirMaps {
		if err := journal.AppendJournalFromDm(dm, dir); err != nil {
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
