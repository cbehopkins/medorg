package consumers

import (
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

	data, err := os.ReadFile(operation.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source: %w", err)
	}
	if err := os.WriteFile(operation.DestinationPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write destination: %w", err)
	}
	return nil
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
