package consumers

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestRestoreContextCopyFailureLeavesTargetPending(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "restore.db")
	targetPath := filepath.Join(tmpDir, "restore", "file.txt")
	const checksum = "test-checksum"
	const size = int64(4)

	err := WithRestoreContext(dbPath, func(restore *RestoreContext) error {
		target := &RestoreTaskTarget{TaskID: "task", TargetAbsPath: targetPath}
		if err := restore.db.InsertPending(target, checksum, size, []string{"VOL1"}); err != nil {
			return err
		}
		result, err := restore.copyFrom("source", false,
			func(string, *RestoreDB) (*CopyPlan, error) {
				return &CopyPlan{Operations: []CopyOperation{{
					SourcePath: "source/file.txt", DestinationPath: targetPath, MD5: checksum, Size: size,
				}}}, nil
			},
			func(*CopyOperation) error { return errors.New("copy failed") },
		)
		if err != nil {
			return err
		}
		if len(result.Failures) != 1 || result.CopiedCount != 0 {
			t.Fatalf("unexpected copy result: %+v", result)
		}
		pendingCount, err := restore.db.CountPending()
		if err != nil {
			return err
		}
		copiedCount, err := restore.db.CountCopied()
		if err != nil {
			return err
		}
		if pendingCount != 1 || copiedCount != 0 {
			t.Fatalf("copy failure changed restore state: pending=%d copied=%d", pendingCount, copiedCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithRestoreContext failed: %v", err)
	}
}

func TestCopyRestoreFileChecksumMismatchRemovesDestination(t *testing.T) {
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "source.bin")
	destinationPath := filepath.Join(tmpDir, "destination.bin")
	if err := os.WriteFile(sourcePath, []byte("source content"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("existing content"), 0o644); err != nil {
		t.Fatalf("write destination: %v", err)
	}

	err := copyRestoreFile(&CopyOperation{
		SourcePath:      sourcePath,
		DestinationPath: destinationPath,
		MD5:             "deliberately-wrong-checksum",
		Size:            int64(len("source content")),
	})
	if err == nil {
		t.Fatal("copyRestoreFile succeeded with an incorrect expected checksum")
	}

	if _, err := os.Stat(destinationPath); !os.IsNotExist(err) {
		t.Fatalf("destination should be removed after failed copy, stat error: %v", err)
	}
}

func TestCopyRestoreFileSizeMismatchRemovesDestination(t *testing.T) {
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "source.bin")
	destinationPath := filepath.Join(tmpDir, "destination.bin")
	if err := os.WriteFile(sourcePath, []byte("source content"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("existing content"), 0o644); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	checksum, err := core.CalcMd5File(tmpDir, filepath.Base(sourcePath))
	if err != nil {
		t.Fatalf("checksum source: %v", err)
	}

	err = copyRestoreFile(&CopyOperation{
		SourcePath:      sourcePath,
		DestinationPath: destinationPath,
		MD5:             checksum,
		Size:            int64(len("source content") + 1),
	})
	if err == nil {
		t.Fatal("copyRestoreFile succeeded with an incorrect expected size")
	}

	if _, err := os.Stat(destinationPath); !os.IsNotExist(err) {
		t.Fatalf("destination should be removed after failed copy, stat error: %v", err)
	}
}

func TestRestoreContextChecksumMismatchLeavesTargetPending(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "restore.db")
	sourcePath := filepath.Join(tmpDir, "source.bin")
	destinationPath := filepath.Join(tmpDir, "restore", "source.bin")
	if err := os.WriteFile(sourcePath, []byte("source content"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	const expectedChecksum = "deliberately-wrong-checksum"
	const expectedSize = int64(len("source content"))

	err := WithRestoreContext(dbPath, func(restore *RestoreContext) error {
		target := &RestoreTaskTarget{TaskID: "task", TargetAbsPath: destinationPath}
		if err := restore.db.InsertPending(target, expectedChecksum, expectedSize, []string{"VOL1"}); err != nil {
			return err
		}

		result, err := restore.copyFrom("source", false,
			func(string, *RestoreDB) (*CopyPlan, error) {
				return &CopyPlan{Operations: []CopyOperation{{
					SourcePath: sourcePath, DestinationPath: destinationPath,
					MD5: expectedChecksum, Size: expectedSize,
				}}}, nil
			}, copyRestoreFile)
		if err != nil {
			return err
		}
		if len(result.Failures) != 1 || result.CopiedCount != 0 {
			t.Fatalf("unexpected copy result: %+v", result)
		}

		pendingCount, err := restore.db.CountPending()
		if err != nil {
			return err
		}
		copiedCount, err := restore.db.CountCopied()
		if err != nil {
			return err
		}
		if pendingCount != 1 || copiedCount != 0 {
			t.Fatalf("checksum failure changed restore state: pending=%d copied=%d", pendingCount, copiedCount)
		}
		if _, err := os.Stat(destinationPath); !os.IsNotExist(err) {
			t.Fatalf("failed copy should remove destination, stat error: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithRestoreContext failed: %v", err)
	}
}

func TestRestoreContextMarksMatchingDestinationCopiedBeforeScanningVolume(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "restore.db")
	destinationPath := filepath.Join(tmpDir, "restore", "source.bin")
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		t.Fatalf("create destination directory: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("already restored"), 0o644); err != nil {
		t.Fatalf("write existing destination: %v", err)
	}
	checksum, err := core.CalcMd5File(filepath.Dir(destinationPath), filepath.Base(destinationPath))
	if err != nil {
		t.Fatalf("checksum existing destination: %v", err)
	}
	const size = int64(len("already restored"))

	err = WithRestoreContext(dbPath, func(restore *RestoreContext) error {
		target := &RestoreTaskTarget{TaskID: "task", TargetAbsPath: destinationPath}
		if err := restore.db.InsertPending(target, checksum, size, []string{"VOL1"}); err != nil {
			return err
		}

		scanCalled := false
		executeCalled := false
		result, err := restore.copyFrom("source", false,
			func(string, *RestoreDB) (*CopyPlan, error) {
				scanCalled = true
				return &CopyPlan{}, nil
			},
			func(*CopyOperation) error {
				executeCalled = true
				return nil
			},
		)
		if err != nil {
			return err
		}
		if scanCalled || executeCalled || result.CopiedCount != 0 {
			t.Fatalf("matching destination should skip scanning and copying: scanCalled=%t executeCalled=%t result=%+v",
				scanCalled, executeCalled, result)
		}

		pendingCount, err := restore.db.CountPending()
		if err != nil {
			return err
		}
		copiedCount, err := restore.db.CountCopied()
		if err != nil {
			return err
		}
		if pendingCount != 0 || copiedCount != 1 {
			t.Fatalf("matching destination was not reconciled: pending=%d copied=%d", pendingCount, copiedCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithRestoreContext failed: %v", err)
	}
}

func TestRestoreContextCopiesRemainingTargetsAfterReconciliation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "restore.db")
	existingPath := filepath.Join(tmpDir, "restore", "existing.bin")
	sourcePath := filepath.Join(tmpDir, "source", "new.bin")
	destinationPath := filepath.Join(tmpDir, "restore", "new.bin")
	if err := os.MkdirAll(filepath.Dir(existingPath), 0o755); err != nil {
		t.Fatalf("create existing destination directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}
	if err := os.WriteFile(existingPath, []byte("already restored"), 0o644); err != nil {
		t.Fatalf("write existing destination: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("copy this file"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	existingChecksum, err := core.CalcMd5File(filepath.Dir(existingPath), filepath.Base(existingPath))
	if err != nil {
		t.Fatalf("checksum existing destination: %v", err)
	}
	sourceChecksum, err := core.CalcMd5File(filepath.Dir(sourcePath), filepath.Base(sourcePath))
	if err != nil {
		t.Fatalf("checksum source: %v", err)
	}
	const existingSize = int64(len("already restored"))
	const sourceSize = int64(len("copy this file"))

	err = WithRestoreContext(dbPath, func(restore *RestoreContext) error {
		if err := restore.db.InsertPending(
			&RestoreTaskTarget{TaskID: "existing", TargetAbsPath: existingPath},
			existingChecksum, existingSize, []string{"VOL1"},
		); err != nil {
			return err
		}
		if err := restore.db.InsertPending(
			&RestoreTaskTarget{TaskID: "new", TargetAbsPath: destinationPath},
			sourceChecksum, sourceSize, []string{"VOL1"},
		); err != nil {
			return err
		}

		scanCalled := false
		result, err := restore.copyFrom("volume", false,
			func(string, *RestoreDB) (*CopyPlan, error) {
				scanCalled = true
				return &CopyPlan{Operations: []CopyOperation{{
					SourcePath: sourcePath, DestinationPath: destinationPath,
					MD5: sourceChecksum, Size: sourceSize,
				}}}, nil
			}, copyRestoreFile)
		if err != nil {
			return err
		}
		if !scanCalled || result.AlreadyPresentCount != 1 || result.CopiedCount != 1 {
			t.Fatalf("remaining target was not copied: scanCalled=%t result=%+v", scanCalled, result)
		}

		pendingCount, err := restore.db.CountPending()
		if err != nil {
			return err
		}
		copiedCount, err := restore.db.CountCopied()
		if err != nil {
			return err
		}
		if pendingCount != 0 || copiedCount != 2 {
			t.Fatalf("unexpected restore state: pending=%d copied=%d", pendingCount, copiedCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithRestoreContext failed: %v", err)
	}

	restored, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatalf("read restored destination: %v", err)
	}
	if string(restored) != "copy this file" {
		t.Fatalf("restored destination mismatch: got %q", restored)
	}
}

func TestRestoreContextIngestJournalPersistsTargets(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "restore.db")
	restorePath := filepath.Join(tmpDir, "restore")
	config, err := core.NewMdConfig(filepath.Join(tmpDir, ".core.xml"))
	if err != nil {
		t.Fatalf("NewMdConfig failed: %v", err)
	}
	if err := config.SetRestoreDestination("test", restorePath); err != nil {
		t.Fatalf("SetRestoreDestination failed: %v", err)
	}

	journal := `<mdj alias="test">
  <dr dir="summer">
    <fr fname="photo.jpg" checksum="checksum123" size="12" bd="VOL_A" />
  </dr>
</mdj>`
	var result JournalIngestResult
	err = WithRestoreContext(dbPath, func(restore *RestoreContext) error {
		var ingestErr error
		result, ingestErr = restore.IngestJournal(bytes.NewReader([]byte(journal)), config)
		return ingestErr
	})
	if err != nil {
		t.Fatalf("IngestJournal failed: %v", err)
	}
	if result.InsertedCount != 1 || result.FailedCount != 0 || result.PendingCount != 1 {
		t.Fatalf("unexpected journal ingest result: %+v", result)
	}

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()
	targets, err := db.FindPendingByContent("checksum123", 12)
	if err != nil {
		t.Fatalf("FindPendingByContent failed: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("expected 1 restore target, got %d", len(targets))
	}

	expectedPath := filepath.Join(restorePath, "summer", "photo.jpg")
	if targets[0].TargetAbsPath != expectedPath {
		t.Fatalf("unexpected target path: want %s got %s", expectedPath, targets[0].TargetAbsPath)
	}
	if targets[0].TaskID != fmt.Sprintf("%s:%d:%s", "checksum123", 12, expectedPath) {
		t.Fatalf("unexpected task id: %s", targets[0].TaskID)
	}

	counts, err := db.CountPendingByBackupDest()
	if err != nil {
		t.Fatalf("CountPendingByBackupDest failed: %v", err)
	}
	if counts["VOL_A"] != 1 {
		t.Fatalf("expected VOL_A count to be 1, got %d", counts["VOL_A"])
	}
}
