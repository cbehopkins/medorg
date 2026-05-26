package consumers

import (
	"fmt"
	"os"

	"github.com/cbehopkins/medorg/pkg/core"
)

// CopyOperation describes a single file copy action: source to one destination.
type CopyOperation struct {
	SourcePath      string // Absolute path to source file to copy from (e.g., /backup/file.txt)
	DestinationPath string // Absolute path where file should be restored (e.g., /restore/photos/file.txt)
	MD5             string // Content identifier (lowercase hex)
	Size            int64  // File size in bytes
	Alias           string // Source alias for logging/tracking
}

// CopyPlan is the result of scanning a source directory against pending restore tasks.
type CopyPlan struct {
	SourcePath  string           // Root source directory scanned
	Operations  []CopyOperation  // Planned copy operations
	UnmatchedCount int            // Number of files in source that don't match any pending task
	MatchedCount   int            // Number of files that match pending tasks
	ErrorCount  int            // Number of files that could not be processed (read errors, etc.)
}

// ScanSourceForPending walks a source directory using DirectoryWalker metadata,
// matches files by (md5, size) against pending tasks, and returns a copy plan.
//
// sourceDir: absolute path to scan (e.g., /backup or /mnt/media)
// db: restore database with pending collection
//
// Returns a CopyPlan with all copy operations needed to restore matched content.
// Errors during file processing are logged but do not stop the scan.
func ScanSourceForPending(sourceDir string, db *RestoreDB) (*CopyPlan, error) {
	if _, err := os.Stat(sourceDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("source directory does not exist: %s", sourceDir)
	}

	plan := &CopyPlan{
		SourcePath: sourceDir,
		Operations: make([]CopyOperation, 0),
	}

	dw := core.NewDirectoryWalker(core.MakeTokenChan(core.NumTrackerOutstanding))
	defer func() { _ = dw.Close() }()

	dw.AddFileVisitor(func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		if core.IsMetadataFile(string(fn)) {
			return nil
		}

		md5Hash := fm.GetChecksum()
		if md5Hash == "" {
			plan.ErrorCount++
			return nil
		}

		targets, err := db.FindPendingByContent(md5Hash, fi.Size())
		if err != nil {
			plan.ErrorCount++
			return nil
		}

		if len(targets) == 0 {
			plan.UnmatchedCount++
			return nil
		}

		for _, target := range targets {
			op := CopyOperation{
				SourcePath:      fm.Path().String(),
				DestinationPath: target.TargetAbsPath,
				MD5:             md5Hash,
				Size:            fi.Size(),
				Alias:           target.Alias,
			}
			plan.Operations = append(plan.Operations, op)
		}

		plan.MatchedCount++
		return nil
	})

	err := dw.Walk(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("error scanning source: %w", err)
	}

	return plan, nil
}
