package consumers

import (
	"errors"
	"path/filepath"
	"testing"
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
