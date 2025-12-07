package consumers

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/cbehopkins/medorg/pkg/adaptive"
	"github.com/cbehopkins/medorg/pkg/core"
)

// CheckCalcOptions configures the mdcalc operation
type CheckCalcOptions struct {
	CalcCount int             // Number of parallel MD5 calculators (default: 2)
	Recalc    bool            // Force recalculation of all checksums
	Validate  bool            // Validate existing checksums
	Scrub     bool            // Remove backup destination tags
	AutoFix   *AutoFix        // Optional auto-fix for file renaming/deletion
	Tuner     *adaptive.Tuner // Optional adaptive tuner for dynamic token adjustment
}

// RunCheckCalc calculates and maintains MD5 checksums for files in the given directories.
// This is the core logic extracted from cmd/mdcalc/main.go
//
// For each directory, it:
// - Loads existing .medorg.xml or creates new one
// - Calculates MD5 checksums for changed files (or all files if Recalc is true)
// - Updates .medorg.xml files
// - Removes entries for deleted files
//
// Returns error if any directory walk or checksum calculation fails.
func RunCheckCalc(directories []string, opts CheckCalcOptions) error {
	// Set defaults
	if opts.CalcCount <= 0 {
		opts.CalcCount = 2
	}

	// Use tuner if provided, otherwise use static token buffer
	var tokenBuffer chan struct{}
	if opts.Tuner != nil {
		opts.Tuner.Start()
		defer opts.Tuner.Stop()
		// We'll use the tuner's token mechanism
	} else {
		// Create token buffer for limiting parallel MD5 calculations
		tokenBuffer = make(chan struct{}, opts.CalcCount)
		defer close(tokenBuffer)
		for i := 0; i < opts.CalcCount; i++ {
			tokenBuffer <- struct{}{}
		}
	}

	// Visitor function - processes each file in each directory
	visitor := func(dm core.DirectoryMap, directory, file string, d fs.DirEntry) error {
		// Skip the .medorg.xml file itself
		if file == core.Md5FileName {
			return nil
		}

		// Process this file
		fc := func(fs *core.FileStruct) error {
			info, err := d.Info()
			if err != nil {
				return err
			}

			// Check if file has changed
			changed, err := fs.Changed(info)
			if err != nil {
				return err
			}

			// Handle scrub flag - remove backup tags
			if opts.Scrub {
				if len(fs.BackupDest) > 0 {
					changed = true
					fs.BackupDest = []string{}
				}
			}

			// Handle validate flag - verify existing checksums
			if opts.Validate {
				if opts.Tuner != nil {
					<-opts.Tuner.AcquireToken()
					defer opts.Tuner.ReleaseToken()

					err = fs.ValidateChecksumWithProgress(func(bytes int64) {
						opts.Tuner.RecordBytes(bytes)
					})
				} else {
					<-tokenBuffer
					defer func() { tokenBuffer <- struct{}{} }()
					err = fs.ValidateChecksum()
				}
				if errors.Is(err, core.ErrRecalced) {
					// Checksum had to be recalculated, but that's ok
					return nil
				}
				return err
			}

			// Skip checksum calculation if not needed
			if !(changed || opts.Recalc || fs.Checksum == "") {
				return nil
			}

			// Update file metadata from stat
			if _, err := fs.FromStat(directory, file, info); err != nil {
				return err
			}

			// Calculate checksum (with concurrency control and optional progress tracking)
			if opts.Tuner != nil {
				<-opts.Tuner.AcquireToken()
				defer opts.Tuner.ReleaseToken()

				err = fs.UpdateChecksumWithProgress(opts.Recalc, func(bytes int64) {
					opts.Tuner.RecordBytes(bytes)
				})
			} else {
				<-tokenBuffer
				defer func() { tokenBuffer <- struct{}{} }()
				err = fs.UpdateChecksum(opts.Recalc)
			}

			if errors.Is(err, ErrIOError) {
				// Log but don't fail on IO errors (file might be locked)
				return nil
			}

			return err
		}

		// Execute the file processing function
		err := dm.RunFsFc(directory, file, fc)
		if err != nil {
			return err
		}

		// Run AutoFix if provided
		if opts.AutoFix != nil {
			if err := opts.AutoFix.WkFun(dm, directory, file, d); err != nil {
				return err
			}
		}

		return nil
	}

	// Maker function - creates DirectoryEntry for each directory
	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			// Load or create DirectoryMap for this directory
			dm, err := core.DirectoryMapFromDir(dir)
			if err != nil {
				return dm, err
			}

			// Set the visitor function
			dm.SetVisitFunc(visitor)

			// Remove entries for files that no longer exist
			return dm, dm.DeleteMissingFiles()
		}
		de, err := core.NewDirectoryEntry(dir, mkFk)
		return de, err
	}

	// Process each directory
	for _, dir := range directories {
		errChan := core.NewDirTracker(false, dir, makerFunc).ErrChan()

		// Check for errors during directory walk
		for err := range errChan {
			return fmt.Errorf("error walking directory %s: %w", dir, err)
		}
	}

	return nil
}
