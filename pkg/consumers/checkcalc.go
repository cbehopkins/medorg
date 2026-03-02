package consumers

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/cbehopkins/medorg/pkg/adaptive"
	"github.com/cbehopkins/medorg/pkg/core"
)

// CheckCalcOptions configures the mdcalc operation
type CheckCalcOptions struct {
	CalcCount    int             // Number of parallel MD5 calculators (default: 2)
	Recalc       bool            // Force recalculation of all checksums
	Validate     bool            // Validate existing checksums
	Scrub        bool            // Remove backup destination tags
	ShowProgress bool            // Show progress during checksum calculation
	AutoFix      *AutoFix        // Optional auto-fix for file renaming/deletion
	Tuner        *adaptive.Tuner // Optional adaptive tuner for dynamic token adjustment
}

// makeScrubMutator returns a mutator that removes backup destination tags from files
func makeScrubMutator() core.DmMutCallback {
	return func(file core.Fpath, d os.FileInfo, fs core.FileStruct) (core.FileStruct, error) {
		if len(fs.BackupDest) > 0 {
			fs.BackupDest = []string{}
			fmt.Println("Scrubbed tags from:", fs.Name)
		}
		return fs, nil
	}
}

// makeValidateMutator returns a mutator that verifies existing checksums
func makeValidateMutator(opts CheckCalcOptions, tokenBuffer chan struct{}) core.DmMutCallback {
	return func(file core.Fpath, d os.FileInfo, fs core.FileStruct) (core.FileStruct, error) {
		if opts.Tuner != nil {
			<-opts.Tuner.AcquireToken()
			defer opts.Tuner.ReleaseToken()
		} else {
			<-tokenBuffer
			defer func() { tokenBuffer <- struct{}{} }()
		}

		err := fs.UpdateChecksum(opts.Recalc, opts.ShowProgress, func(bytes int64) {
			if opts.Tuner != nil {
				opts.Tuner.RecordBytes(bytes)
			}
		})

		if errors.Is(err, core.ErrRecalced) {
			// Checksum had to be recalculated, but that's ok
			return fs, nil
		}
		return fs, err
	}
}

// makeChecksumMutator returns a mutator that calculates checksums for changed/new files
func makeChecksumMutator(opts CheckCalcOptions, tokenBuffer chan struct{}) core.DmMutCallback {
	return func(file core.Fpath, d os.FileInfo, fs core.FileStruct) (core.FileStruct, error) {
		// Check if file has changed
		changed, err := fs.Changed(d)
		if err != nil {
			return fs, err
		}

		// Skip checksum calculation if not needed
		if !(changed || opts.Recalc || fs.Checksum == "") {
			return fs, core.ErrIgnoreThisMutate
		}

		// Update file metadata from stat
		fs, err = fs.FromStat(file.Dir(), file.Base(), d)
		if err != nil {
			return fs, err
		}

		// Calculate checksum (with concurrency control and optional progress tracking)
		if opts.Tuner != nil {
			<-opts.Tuner.AcquireToken()
			defer opts.Tuner.ReleaseToken()
		} else {
			<-tokenBuffer
			defer func() { tokenBuffer <- struct{}{} }()
		}

		err = fs.UpdateChecksum(opts.Recalc, opts.ShowProgress, func(bytes int64) {
			if opts.Tuner != nil {
				opts.Tuner.RecordBytes(bytes)
			}
		})

		if errors.Is(err, ErrIOError) {
			// Log but don't fail on IO errors (file might be locked)
			return fs, core.ErrIgnoreThisMutate
		}

		return fs, err
	}
}

// makeAutoFixVisitor returns a visitor that applies AutoFix to files
func makeAutoFixVisitor(autoFix *AutoFix) core.ForEachCallback {
	return func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		fsPtr, ok := fm.(*core.FileStruct)
		if !ok {
			return nil
		}
		fp := core.NewFpath(string(fsPtr.Directory()), string(fn))

		// Create a DirEntry wrapper from FileInfo
		dirEntry := &fileInfoDirEntry{fi: fi}

		// Get DirectoryMap - we need to create it temporarily for AutoFix
		dm, err := core.DirectoryMapFromDir(fsPtr.Directory(), nil)
		if err != nil {
			return err
		}

		return autoFix.WkFun(dm, fp.Dir(), fp.Base(), dirEntry)
	}
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

	// Create DirectoryWalker with mutators for each operation
	dw := core.NewDirectoryWalker(core.MakeTokenChan(core.NumTrackerOutstanding))
	defer dw.Close()

	// Add appropriate mutators based on flags
	if opts.Scrub {
		dw.AddFileMutator(makeScrubMutator())
	}

	if opts.Validate {
		dw.AddFileMutator(makeValidateMutator(opts, tokenBuffer))
	} else {
		dw.AddFileMutator(makeChecksumMutator(opts, tokenBuffer))
	}

	if opts.AutoFix != nil {
		dw.AddFileVisitor(makeAutoFixVisitor(opts.AutoFix))
	}

	// Process each directory
	var retErr error
	for _, dir := range directories {
		if err := dw.Walk(dir); err != nil {
			retErr = err
		}
	}

	return retErr
}

// fileInfoDirEntry wraps os.FileInfo to implement fs.DirEntry
type fileInfoDirEntry struct {
	fi os.FileInfo
}

func (f *fileInfoDirEntry) Name() string               { return f.fi.Name() }
func (f *fileInfoDirEntry) IsDir() bool                { return f.fi.IsDir() }
func (f *fileInfoDirEntry) Type() fs.FileMode          { return f.fi.Mode().Type() }
func (f *fileInfoDirEntry) Info() (os.FileInfo, error) { return f.fi, nil }
