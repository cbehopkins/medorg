package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
)

func TestCopySubcommand_TriggersCopyFromPlan(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	dbPath := filepath.Join(tmpDir, "restore.db")
	configPath := filepath.Join(tmpDir, "test.mdcfg.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	db, err := consumers.OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	targets := []struct {
		path string
		md5  string
		size int64
	}{
		{path: "/restore/one.txt", md5: "hash1", size: 10},
		{path: "/restore/two.txt", md5: "hash2", size: 20},
	}
	for _, tc := range targets {
		target := &consumers.RestoreTaskTarget{TaskID: tc.md5 + ":task:" + tc.path, Alias: "test", TargetAbsPath: tc.path}
		if err := db.InsertPending(target, tc.md5, tc.size, []string{"VOL1"}); err != nil {
			t.Fatalf("InsertPending failed: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	originalScan := scanSourceForPendingFunc
	originalExecute := executeCopyOperationFunc
	defer func() {
		scanSourceForPendingFunc = originalScan
		executeCopyOperationFunc = originalExecute
	}()

	scanSourceForPendingFunc = func(source string, db *consumers.RestoreDB) (*consumers.CopyPlan, error) {
		return &consumers.CopyPlan{
			SourcePath:     source,
			MatchedCount:   2,
			UnmatchedCount: 0,
			ErrorCount:     0,
			Operations: []consumers.CopyOperation{
				{SourcePath: filepath.Join(sourceDir, "source1.bin"), DestinationPath: "/restore/one.txt", MD5: "hash1", Size: 10, Alias: "test"},
				{SourcePath: filepath.Join(sourceDir, "source2.bin"), DestinationPath: "/restore/two.txt", MD5: "hash2", Size: 20, Alias: "test"},
			},
		}, nil
	}

	copyCalls := 0
	executeCopyOperationFunc = func(op *consumers.CopyOperation) error {
		copyCalls++
		return nil
	}

	exitCode, err := copySubcommand([]string{"-config", configPath, "-db", dbPath, sourceDir})
	if err != nil {
		t.Fatalf("copySubcommand returned error: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Fatalf("expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
	if copyCalls != 2 {
		t.Fatalf("expected 2 copy calls, got %d", copyCalls)
	}
}
