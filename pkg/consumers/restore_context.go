package consumers

import (
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	core "github.com/cbehopkins/medorg/pkg/core"
)

// RestoreContext coordinates journal intake and copy progress for one active restore.
type RestoreContext struct {
	db *RestoreDB
}

// JournalIngestResult summarises targets loaded from a journal.
type JournalIngestResult struct {
	InsertedCount int
	FailedCount   int
	PendingCount  int
	Warnings      []JournalIngestWarning
}

// JournalIngestWarning describes a target that could not be inserted.
type JournalIngestWarning struct {
	FileName string
	Err      error
}

// CopyResult summarises one source-volume scan and its copy attempts.
type CopyResult struct {
	Plan                *CopyPlan
	CopiedCount         int
	Failures            []CopyFailure
	PendingByBackupDest map[string]int
}

// CopyFailure records whether a copy or its subsequent state update failed.
type CopyFailure struct {
	Operation   CopyOperation
	StateUpdate bool
	Err         error
}

// WithRestoreContext opens a restore database for one workflow phase and closes it afterward.
func WithRestoreContext(filePath string, fn func(*RestoreContext) error) error {
	return WithRestoreDB(filePath, func(db *RestoreDB) error {
		return fn(&RestoreContext{db: db})
	})
}

// IngestJournal loads journal targets and reports the resulting pending count.
func (restore *RestoreContext) IngestJournal(journal io.Reader, config *core.MdConfig) (JournalIngestResult, error) {
	result, err := ingestJournalAndInsert(journal, restore.db, config)
	if err != nil {
		return result, err
	}
	result.PendingCount, err = restore.db.CountPending()
	if err != nil {
		return result, fmt.Errorf("count pending restore tasks: %w", err)
	}
	return result, nil
}

// CopyFrom scans a backup volume and copies its matches into the restore destinations.
func (restore *RestoreContext) CopyFrom(sourceDir string, dryRun bool) (*CopyResult, error) {
	return restore.copyFrom(sourceDir, dryRun, ScanSourceForPending, copyRestoreFile)
}

func (restore *RestoreContext) copyFrom(
	sourceDir string,
	dryRun bool,
	scan func(string, *RestoreDB) (*CopyPlan, error),
	execute func(*CopyOperation) error,
) (*CopyResult, error) {
	plan, err := scan(sourceDir, restore.db)
	if err != nil {
		return nil, fmt.Errorf("failed to scan source: %w", err)
	}
	result := &CopyResult{Plan: plan}
	if dryRun {
		return result, nil
	}

	for operationIndex := range plan.Operations {
		operation := &plan.Operations[operationIndex]
		if err := execute(operation); err != nil {
			result.Failures = append(result.Failures, CopyFailure{Operation: *operation, Err: err})
			continue
		}
		if err := restore.db.MoveToCopied(operation.DestinationPath, operation.MD5, operation.Size); err != nil {
			result.Failures = append(result.Failures, CopyFailure{Operation: *operation, StateUpdate: true, Err: err})
			continue
		}
		result.CopiedCount++
	}

	result.PendingByBackupDest, _ = restore.db.CountPendingByBackupDest()
	return result, nil
}

func copyRestoreFile(operation *CopyOperation) error {
	destDir := filepath.Dir(operation.DestinationPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	source, err := os.Open(operation.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}
	defer source.Close()

	sourceInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("failed to inspect source: %w", err)
	}
	if destinationInfo, err := os.Stat(operation.DestinationPath); err == nil {
		if os.SameFile(sourceInfo, destinationInfo) {
			return fmt.Errorf("source and destination refer to the same file")
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to inspect destination: %w", err)
	}

	destination, err := os.OpenFile(operation.DestinationPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open destination: %w", err)
	}
	checksum := md5.New()
	copiedSize, copyErr := io.Copy(io.MultiWriter(destination, checksum), source)
	closeErr := destination.Close()
	if copyErr != nil {
		return removeFailedRestoreCopy(operation.DestinationPath, fmt.Errorf("failed to copy source to destination: %w", copyErr))
	}
	if closeErr != nil {
		return removeFailedRestoreCopy(operation.DestinationPath, fmt.Errorf("failed to close destination: %w", closeErr))
	}
	if copiedSize != operation.Size {
		return removeFailedRestoreCopy(operation.DestinationPath,
			fmt.Errorf("copied size mismatch: got %d bytes, expected %d", copiedSize, operation.Size))
	}
	actualChecksum := core.ReturnChecksumString(checksum)
	if actualChecksum != operation.MD5 {
		return removeFailedRestoreCopy(operation.DestinationPath,
			fmt.Errorf("checksum mismatch: got %s, expected %s", actualChecksum, operation.MD5))
	}
	return nil
}

func removeFailedRestoreCopy(destinationPath string, cause error) error {
	if err := os.Remove(destinationPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%w; failed to remove incomplete destination: %v", cause, err)
	}
	return cause
}

func ingestJournalAndInsert(journal io.Reader, db *RestoreDB, config *core.MdConfig) (JournalIngestResult, error) {
	result := JournalIngestResult{}
	decoder := xml.NewDecoder(journal)
	var currentAlias string
	var currentDir string

	for {
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return result, fmt.Errorf("error reading journal XML: %w", err)
		}

		startElem, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		switch startElem.Name.Local {
		case "mdj":
			for _, attr := range startElem.Attr {
				if attr.Name.Local == "alias" {
					currentAlias = attr.Value
					break
				}
			}

		case "dr":
			for _, attr := range startElem.Attr {
				if attr.Name.Local == "dir" {
					currentDir = attr.Value
					break
				}
			}

		case "fr":
			if currentAlias == "" {
				result.FailedCount++
				continue
			}

			restoreBasePath, found := config.GetRestoreDestination(currentAlias)
			if !found {
				if sourceDir, ok := config.GetSourceDirectory(currentAlias); ok {
					restoreBasePath = sourceDir.Path
					found = true
				}
			}
			if !found || restoreBasePath == "" {
				result.FailedCount++
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
					backupDests = append(backupDests, attr.Value)
				}
			}

			targetAbsPath := filepath.Join(restoreBasePath, filepath.Base(currentDir), fileName)
			taskID := fmt.Sprintf("%s:%d:%s", checksum, size, targetAbsPath)
			target := &RestoreTaskTarget{
				TaskID:        taskID,
				Alias:         currentAlias,
				TargetAbsPath: targetAbsPath,
				CreatedAtUnix: time.Now().Unix(),
			}
			if err := db.InsertPending(target, checksum, size, backupDests); err != nil {
				result.FailedCount++
				result.Warnings = append(result.Warnings, JournalIngestWarning{FileName: fileName, Err: err})
				continue
			}
			result.InsertedCount++
		}
	}

	return result, nil
}
