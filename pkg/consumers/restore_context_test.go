package consumers

import (
	"errors"
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
