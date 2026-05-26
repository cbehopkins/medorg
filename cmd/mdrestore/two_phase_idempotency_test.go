package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

func TestTwoPhaseRestoreCopyIsIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	journalPath := filepath.Join(tmpDir, "journal.xml")
	dbPath := filepath.Join(tmpDir, "restore.db")
	configPath := filepath.Join(tmpDir, "test.mdcfg.xml")

	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	fileName := "movie.mkv"
	fileData := []byte("idempotency-test-content")
	filePath := filepath.Join(sourceDir, fileName)
	if err := os.WriteFile(filePath, fileData, 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	checksum, err := core.CalcMd5File(sourceDir, fileName)
	if err != nil {
		t.Fatalf("CalcMd5File failed: %v", err)
	}

	xc, err := core.NewMdConfig(configPath)
	if err != nil {
		t.Fatalf("NewMdConfig failed: %v", err)
	}
	if !xc.AddSourceDirectory(sourceDir, "test") {
		t.Fatalf("failed to add source directory alias")
	}
	if err := xc.SetRestoreDestination("test", filepath.Join(tmpDir, "restored")); err != nil {
		t.Fatalf("SetRestoreDestination failed: %v", err)
	}
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("WriteXmlCfg failed: %v", err)
	}

	journalContent := fmt.Sprintf(`<mdj alias="test">
  <dr dir="summer">
    <fr fname="%s" checksum="%s" size="%d" bd="VOL1" />
  </dr>
</mdj>`, fileName, checksum, len(fileData))
	if err := os.WriteFile(journalPath, []byte(journalContent), 0o644); err != nil {
		t.Fatalf("failed to write journal: %v", err)
	}

	exitCode, err := newdbSubcommand([]string{"-config", configPath, "-journal", journalPath, "-db", dbPath})
	if err != nil {
		t.Fatalf("newdbSubcommand returned error: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Fatalf("newdbSubcommand exit code mismatch: got %d want %d", exitCode, cli.ExitOk)
	}

	originalCopy := executeCopyOperationFunc
	defer func() { executeCopyOperationFunc = originalCopy }()

	copyCalls := 0
	executeCopyOperationFunc = func(op *consumers.CopyOperation) error {
		copyCalls++
		return nil
	}

	exitCode, err = copySubcommand([]string{"-config", configPath, "-db", dbPath, sourceDir})
	if err != nil {
		t.Fatalf("first copySubcommand returned error: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Fatalf("first copySubcommand exit code mismatch: got %d want %d", exitCode, cli.ExitOk)
	}
	if copyCalls != 1 {
		t.Fatalf("expected first copy pass to execute 1 copy, got %d", copyCalls)
	}

	db, err := consumers.OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	pendingCount, err := db.CountPending()
	if err != nil {
		t.Fatalf("CountPending failed: %v", err)
	}
	copiedCount, err := db.CountCopied()
	if err != nil {
		t.Fatalf("CountCopied failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if pendingCount != 0 || copiedCount != 1 {
		t.Fatalf("unexpected DB state after first copy: pending=%d copied=%d", pendingCount, copiedCount)
	}

	copyCalls = 0
	exitCode, err = copySubcommand([]string{"-config", configPath, "-db", dbPath, sourceDir})
	if err != nil {
		t.Fatalf("second copySubcommand returned error: %v", err)
	}
	if exitCode != cli.ExitOk {
		t.Fatalf("second copySubcommand exit code mismatch: got %d want %d", exitCode, cli.ExitOk)
	}
	if copyCalls != 0 {
		t.Fatalf("expected second copy pass to execute 0 copies, got %d", copyCalls)
	}

	db, err = consumers.OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed on second check: %v", err)
	}
	pendingCount, err = db.CountPending()
	if err != nil {
		t.Fatalf("CountPending failed on second check: %v", err)
	}
	copiedCount, err = db.CountCopied()
	if err != nil {
		t.Fatalf("CountCopied failed on second check: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed on second check: %v", err)
	}

	if pendingCount != 0 || copiedCount != 1 {
		t.Fatalf("DB state changed after second copy pass: pending=%d copied=%d", pendingCount, copiedCount)
	}
}
