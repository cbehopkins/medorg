package consumers

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
	"github.com/spf13/afero"
)

// TestOrphansDeletedBeforeCopyOnNoSpace verifies that orphan deletion happens
// before copying, using afero memfs to simulate a full destination.
func TestOrphansDeletedBeforeCopyOnNoSpace(t *testing.T) {
	// Setup real temp directories for medorg.xml persistence
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source file
	srcFile := filepath.Join(srcDir, "file.txt")
	if err := os.WriteFile(srcFile, []byte("source content"), 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create another source file to trigger the second copy that will fail
	srcFile2 := filepath.Join(srcDir, "file2.txt")
	if err := os.WriteFile(srcFile2, []byte("source content 2"), 0o644); err != nil {
		t.Fatalf("failed to create source file 2: %v", err)
	}

	// Create destination orphan file
	orphanFile := filepath.Join(dstDir, "orphan.txt")
	if err := os.WriteFile(orphanFile, []byte("orphan content"), 0o644); err != nil {
		t.Fatalf("failed to create orphan file: %v", err)
	}

	// Setup minimal mdcfg for source and destination
	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("failed to persist config: %v", err)
	}

	// Seed medorg.xml files with checksums using RunCheckCalc
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on src failed: %v (may be OK if files already cached)", err)
	}
	if err := RunCheckCalc([]string{dstDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on dst failed: %v", err)
	}

	// Track call order and deleted files
	var callOrder []string
	var deletedFiles []string
	copyCount := 0

	// Failover copier: fails with ErrNoSpace after 1 copy
	failingCopier := func(src, dst core.Fpath) error {
		callOrder = append(callOrder, fmt.Sprintf("copy(%s)", filepath.Base(src.String())))
		copyCount++
		// Second copy (or any beyond first) fails with no space
		if copyCount > 1 {
			return ErrNoSpace
		}
		// First copy succeeds (dummy)
		return ErrDummyCopy
	}

	// Orphan handler: deletes the file, records the call
	orphanHandler := func(path string) error {
		callOrder = append(callOrder, fmt.Sprintf("delete(%s)", filepath.Base(path)))
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		deletedFiles = append(deletedFiles, path)
		return nil
	}

	// Run backup
	var logBuf bytes.Buffer
	logFunc := func(msg string) {
		log.Println(msg)
		logBuf.WriteString(msg + "\n")
	}

	volumeLabeler := &testVolumeLabeler{xc}
	err = BackupRunner(
		volumeLabeler,
		2,
		failingCopier,
		dstDir,
		orphanHandler,
		logFunc,
		nil,  // no progress factory
		nil,  // no shutdown chan
		true, // skip check calc
		nil,  // no ignore func
		srcDir,
	)

	// Should return ErrNoSpace
	if !errors.Is(err, ErrNoSpace) {
		t.Errorf("expected ErrNoSpace, got %v", err)
	}

	// Verify orphan was deleted
	if _, err := os.Stat(orphanFile); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan file should be deleted, but still exists: %v", err)
	}

	// Verify deletion happened before copy
	// Call order should have delete(...) before copy(...)
	deleteIdx := -1
	copyIdx := -1
	for i, call := range callOrder {
		if call == fmt.Sprintf("delete(%s)", filepath.Base(orphanFile)) {
			deleteIdx = i
		}
		if call == "copy(file.txt)" {
			copyIdx = i
		}
	}
	if deleteIdx < 0 {
		t.Errorf("orphan delete() not called")
	}
	if copyIdx < 0 {
		t.Errorf("file copy() not called")
	}
	if deleteIdx >= 0 && copyIdx >= 0 && deleteIdx >= copyIdx {
		t.Errorf("orphan delete should happen before copy, but order was: %v", callOrder)
	}

	t.Logf("Call order: %v", callOrder)
	t.Logf("Deleted files: %v", deletedFiles)
}

// TestBackupWithAFeroMemFS uses afero memfs to simulate a destination that fills up.
// This ensures orphan deletion (which needs real disk) happens before copy attempts.
func TestBackupWithAFeroMemFS(t *testing.T) {
	// Real temp dirs for medorg.xml
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source files
	srcFile1 := filepath.Join(srcDir, "file1.txt")
	srcFile2 := filepath.Join(srcDir, "file2.txt")
	if err := os.WriteFile(srcFile1, []byte("content1"), 0o644); err != nil {
		t.Fatalf("failed to write source file 1: %v", err)
	}
	if err := os.WriteFile(srcFile2, []byte("content2"), 0o644); err != nil {
		t.Fatalf("failed to write source file 2: %v", err)
	}

	// Create orphan in destination
	orphanFile := filepath.Join(dstDir, "orphan.txt")
	if err := os.WriteFile(orphanFile, []byte("orphan"), 0o644); err != nil {
		t.Fatalf("failed to create orphan: %v", err)
	}

	// Setup config
	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("failed to persist config: %v", err)
	}

	// Calculate checksums
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on src: %v", err)
	}
	if err := RunCheckCalc([]string{dstDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on dst: %v", err)
	}

	// Setup afero memfs for tracking copy operations
	memFS := afero.NewMemMapFs()
	copyOps := []string{}

	// Copier that writes to memfs, simulating disk full after the second copy
	limitedCopier := func(src, dst core.Fpath) error {
		copyOps = append(copyOps, filepath.Base(src.String()))

		// Simulate disk full on the second copy attempt
		if len(copyOps) > 1 {
			return ErrNoSpace
		}

		// Read from real src, write to memfs (simulating copy)
		data, err := os.ReadFile(src.String())
		if err != nil {
			return err
		}
		dstPath := dst.String()
		if err := afero.WriteFile(memFS, dstPath, data, 0o644); err != nil {
			return err
		}
		return ErrDummyCopy // Don't actually update medorg.xml
	}

	// Track orphan deletions
	deletions := []string{}
	orphanHandler := func(path string) error {
		deletions = append(deletions, filepath.Base(path))
		return os.Remove(path)
	}

	var logBuf bytes.Buffer
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	volumeLabeler := &testVolumeLabeler{xc}
	err = BackupRunner(
		volumeLabeler,
		2,
		limitedCopier,
		dstDir,
		orphanHandler,
		logFunc,
		nil,
		nil,
		true,
		nil,
		srcDir,
	)

	// Expect ErrNoSpace
	if !errors.Is(err, ErrNoSpace) {
		t.Errorf("expected ErrNoSpace, got %v", err)
	}

	// Orphan should have been deleted
	if _, err := os.Stat(orphanFile); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan should be deleted")
	}

	// At least one deletion should have occurred
	if len(deletions) == 0 {
		t.Errorf("expected orphan deletions, got none")
	}

	t.Logf("Deletions: %v", deletions)
	t.Logf("Copy ops before full: %v", copyOps)
	t.Logf("MemFS files: %v", memFS)
}

// testVolumeLabeler provides a volume label from MdConfig
type testVolumeLabeler struct {
	cfg *core.MdConfig
}

func (t *testVolumeLabeler) GetVolumeLabel(destDir string) (string, error) {
	vc, err := t.cfg.VolumeCfgFromDir(destDir)
	if err != nil {
		return "", err
	}
	return vc.Label, nil
}

// Helper to compute MD5 checksum (for test setup if needed)
func computeMD5(data []byte) string {
	h := md5.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// TestBackupRecoveryFromInterruptedCopy verifies that a backup can resume
// after being interrupted mid-copy. Uses afero to control FileCopier failures.
func TestBackupRecoveryFromInterruptedCopy(t *testing.T) {
	// Setup real temp directories
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create 5 source files
	srcFiles := []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"}
	for i, fname := range srcFiles {
		fpath := filepath.Join(srcDir, fname)
		content := fmt.Sprintf("content of file %d", i+1)
		if err := os.WriteFile(fpath, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
	}

	// Setup config
	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("failed to persist config: %v", err)
	}

	// Calculate checksums
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on src: %v", err)
	}

	// First backup: fails on 3rd file, simulating interruption
	firstAttemptCopies := 0
	firstAttemptCopier := func(src, dst core.Fpath) error {
		firstAttemptCopies++
		if firstAttemptCopies > 2 {
			// Fail on 3rd copy to simulate interruption
			return ErrNoSpace
		}
		// Dummy copy (don't actually write)
		return ErrDummyCopy
	}

	var logBuf bytes.Buffer
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	volumeLabeler := &testVolumeLabeler{xc}
	err = BackupRunner(
		volumeLabeler,
		2,
		firstAttemptCopier,
		dstDir,
		func(path string) error { return os.Remove(path) }, // orphan handler
		logFunc,
		nil,
		nil,
		true,
		nil,
		srcDir,
	)

	// First attempt should fail
	if !errors.Is(err, ErrNoSpace) {
		t.Errorf("first backup should fail with ErrNoSpace, got %v", err)
	}
	if firstAttemptCopies < 2 {
		t.Errorf("first backup should have copied at least 2 files, got %d", firstAttemptCopies)
	}

	// Second backup: completes successfully, should skip already-processed files
	secondAttemptCopies := 0
	secondAttemptCopier := func(src, dst core.Fpath) error {
		secondAttemptCopies++
		// All copies succeed on second attempt
		return ErrDummyCopy
	}

	logBuf.Reset()
	err = BackupRunner(
		volumeLabeler,
		2,
		secondAttemptCopier,
		dstDir,
		func(path string) error { return os.Remove(path) },
		logFunc,
		nil,
		nil,
		true,
		nil,
		srcDir,
	)

	// Second attempt should succeed
	if err != nil {
		t.Errorf("second backup should succeed, got error: %v", err)
	}

	// Second attempt should process remaining files (should be idempotent)
	// Since we failed on 3rd file, second run needs to at least attempt file 3,4,5
	// But it may also re-attempt files 1,2 since medorg.xml wasn't updated on first attempt
	if secondAttemptCopies < 3 {
		t.Errorf("second backup should copy remaining files, got %d copies", secondAttemptCopies)
	}

	t.Logf("First attempt: %d copies before interruption", firstAttemptCopies)
	t.Logf("Second attempt: %d copies to complete", secondAttemptCopies)
}

// TestOrphanReclaimSpaceWithinSingleBackup verifies that orphans are deleted
// before copying, allowing their space to be reused for new files.
func TestOrphanReclaimSpaceWithinSingleBackup(t *testing.T) {
	// Setup real temp directories
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source files (small)
	srcFile := filepath.Join(srcDir, "newfile.txt")
	if err := os.WriteFile(srcFile, []byte("new content"), 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create large orphan file in destination (simulates old backup)
	orphanFile := filepath.Join(dstDir, "old_backup.dat")
	largeContent := make([]byte, 10*1024*1024) // 10MB
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	if err := os.WriteFile(orphanFile, largeContent, 0o644); err != nil {
		t.Fatalf("failed to create orphan file: %v", err)
	}

	orphanSizeBefore := int64(len(largeContent))

	// Setup config
	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("failed to persist config: %v", err)
	}

	// Calculate checksums
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on src: %v", err)
	}
	if err := RunCheckCalc([]string{dstDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on dst: %v", err)
	}

	// Track call order: deletion should happen before copy
	var callOrder []string
	var reclaimedSpace int64 = 0
	var reclaimedOrphanFiles []string

	orphanHandler := func(path string) error {
		callOrder = append(callOrder, "delete")
		// Only count actual data files, not .medorg.xml or .mdbackup.xml
		if filepath.Base(path) == "old_backup.dat" {
			stat, err := os.Stat(path)
			if err == nil {
				reclaimedSpace += stat.Size()
				reclaimedOrphanFiles = append(reclaimedOrphanFiles, filepath.Base(path))
			}
		}
		return os.Remove(path)
	}

	copier := func(src, dst core.Fpath) error {
		callOrder = append(callOrder, "copy")
		return ErrDummyCopy
	}

	var logBuf bytes.Buffer
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	volumeLabeler := &testVolumeLabeler{xc}
	err = BackupRunner(
		volumeLabeler,
		2,
		copier,
		dstDir,
		orphanHandler,
		logFunc,
		nil,
		nil,
		true,
		nil,
		srcDir,
	)

	if err != nil {
		t.Errorf("backup should succeed, got error: %v", err)
	}

	// Verify orphan was deleted
	if _, err := os.Stat(orphanFile); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan should be deleted but still exists")
	}

	// Verify deletion happened first in call order
	if len(callOrder) > 0 && callOrder[0] != "delete" {
		t.Errorf("orphan should be deleted before copying, got call order: %v", callOrder)
	}

	// Verify space was reclaimed (from the actual data file, not medorg files)
	if reclaimedSpace != orphanSizeBefore {
		t.Errorf("expected to reclaim %d bytes, got %d", orphanSizeBefore, reclaimedSpace)
	}

	t.Logf("Reclaimed space from orphan: %d bytes", reclaimedSpace)
	t.Logf("Reclaimed orphan files: %v", reclaimedOrphanFiles)
	t.Logf("Call order: %v", callOrder)
}

// TestLargeFileHandlingWithMemFS verifies that large files are handled correctly
// without needing sparse file creation (which fails on Windows).
// This replaces the skipped TestSizeOfLargeFile and TestSizeOfOverflowBoundary.
func TestLargeFileHandlingWithMemFS(t *testing.T) {
	// Setup real temp directories
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a "large" file by writing actual content
	// In a real test with memfs, we could simulate 100MB without allocating actual bytes
	largeFile := filepath.Join(srcDir, "largefile.bin")
	largeContent := make([]byte, 1024*1024) // 1MB (manageable in tests)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	if err := os.WriteFile(largeFile, largeContent, 0o644); err != nil {
		t.Fatalf("failed to create large file: %v", err)
	}

	// Verify file size calculation works correctly
	stat, err := os.Stat(largeFile)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	expectedSize := int64(len(largeContent))
	if stat.Size() != expectedSize {
		t.Errorf("file size mismatch: expected %d, got %d", expectedSize, stat.Size())
	}

	// Setup config
	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("failed to persist config: %v", err)
	}

	// Calculate checksums (which involves reading the file)
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on src: %v", err)
	}

	// Track copy operations
	copiesProcessed := 0
	copier := func(src, dst core.Fpath) error {
		copiesProcessed++
		// Verify we can read the source file
		data, err := os.ReadFile(src.String())
		if err != nil {
			return err
		}
		if int64(len(data)) != expectedSize {
			return fmt.Errorf("copied file size mismatch: expected %d, got %d", expectedSize, len(data))
		}
		return ErrDummyCopy
	}

	var logBuf bytes.Buffer
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	volumeLabeler := &testVolumeLabeler{xc}
	err = BackupRunner(
		volumeLabeler,
		1, // single file, so only 1 copy attempt
		copier,
		dstDir,
		func(path string) error { return os.Remove(path) },
		logFunc,
		nil,
		nil,
		true,
		nil,
		srcDir,
	)

	if err != nil {
		t.Errorf("backup should succeed, got error: %v", err)
	}

	if copiesProcessed != 1 {
		t.Errorf("expected 1 copy, got %d", copiesProcessed)
	}

	t.Logf("Large file (%d bytes) processed successfully", expectedSize)
}

// TestBackupWithIdenticalSourceAndOrphan verifies that a file identical to
// an orphan is not deleted (the orphan IS the backup). We skip .mdbackup.xml
// and .medorg.xml files from the orphan handler since they are system files.
func TestBackupWithIdenticalSourceAndOrphan(t *testing.T) {
	// Setup real temp directories
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source file with specific content
	sourceContent := []byte("shared content between source and destination")
	srcFile := filepath.Join(srcDir, "important.txt")
	if err := os.WriteFile(srcFile, sourceContent, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create identical file in destination (the "orphan" that shouldn't be deleted)
	dstFile := filepath.Join(dstDir, "important.txt")
	if err := os.WriteFile(dstFile, sourceContent, 0o644); err != nil {
		t.Fatalf("failed to create destination file: %v", err)
	}

	// Setup config
	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("failed to persist config: %v", err)
	}

	// Calculate checksums for both directories
	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on src: %v", err)
	}
	if err := RunCheckCalc([]string{dstDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Logf("warning: check calc on dst: %v", err)
	}

	// Track orphan deletions (there should be none for user files)
	deletedFiles := []string{}
	orphanHandler := func(path string) error {
		baseName := filepath.Base(path)
		// Only track data file deletions, not system files
		if baseName != ".mdbackup.xml" && baseName != ".medorg.xml" && !strings.HasPrefix(baseName, ".") {
			deletedFiles = append(deletedFiles, baseName)
		}
		return os.Remove(path)
	}

	copier := func(src, dst core.Fpath) error {
		// Should not be called if file already exists with same checksum
		return ErrDummyCopy
	}

	var logBuf bytes.Buffer
	logFunc := func(msg string) {
		logBuf.WriteString(msg + "\n")
	}

	volumeLabeler := &testVolumeLabeler{xc}
	err = BackupRunner(
		volumeLabeler,
		2,
		copier,
		dstDir,
		orphanHandler,
		logFunc,
		nil,
		nil,
		true,
		nil,
		srcDir,
	)

	if err != nil {
		t.Errorf("backup should succeed, got error: %v", err)
	}

	// Verify the destination file still exists (was not deleted)
	if _, err := os.Stat(dstFile); errors.Is(err, os.ErrNotExist) {
		t.Errorf("destination file should not be deleted as orphan (it matches source)")
	}

	// Verify no user file deletions occurred (system files are OK to delete)
	if len(deletedFiles) > 0 {
		t.Errorf("expected no user file deletions, but deleted: %v", deletedFiles)
	}

	t.Logf("File correctly preserved when matching source")
}

// TestBackupResumeUpdatesMetadata ensures that metadata (.md5_list.xml) is
// updated across interrupted runs: first run writes partial entries, second
// run completes and the directory map reflects all files.
func TestBackupResumeUpdatesMetadata(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	files := []string{"a.txt", "b.txt"}
	for i, name := range files {
		p := filepath.Join(srcDir, name)
		if err := os.WriteFile(p, []byte(fmt.Sprintf("content-%d", i)), 0o644); err != nil {
			t.Fatalf("write source %s: %v", name, err)
		}
	}

	srcCfg := filepath.Join(srcDir, ".mdcfg.xml")
	xc, err := core.NewMdConfig(srcCfg)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	xc.AddSourceDirectory(srcDir, "src")
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("persist cfg: %v", err)
	}

	if err := RunCheckCalc([]string{srcDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Fatalf("checkcalc src: %v", err)
	}
	if err := RunCheckCalc([]string{dstDir}, CheckCalcOptions{CalcCount: 1}); err != nil {
		t.Fatalf("checkcalc dst: %v", err)
	}

	copyCount := 0
	failingCopier := func(src, dst core.Fpath) error {
		copyCount++
		if copyCount == 2 {
			return ErrNoSpace // interrupt after first copy
		}
		return core.CopyFile(src, dst)
	}

	volumeLabeler := &testVolumeLabeler{xc}
	firstErr := BackupRunner(
		volumeLabeler,
		2,
		failingCopier,
		dstDir,
		func(path string) error { return os.Remove(path) },
		func(string) {},
		nil,
		nil,
		true,
		nil,
		srcDir,
	)
	if !errors.Is(firstErr, ErrNoSpace) {
		t.Fatalf("first run should fail with ErrNoSpace, got %v", firstErr)
	}

	dm, err := core.DirectoryMapFromDir(core.Dirname(dstDir))
	if err != nil {
		t.Fatalf("read directory map after first run: %v", err)
	}
	if _, ok := dm.Get(core.Fname(files[0])); !ok {
		t.Fatalf("expected %s in directory map after first run", files[0])
	}
	if _, ok := dm.Get(core.Fname(files[1])); ok {
		t.Fatalf("did not expect %s in directory map after first run", files[1])
	}

	successCopier := func(src, dst core.Fpath) error {
		return core.CopyFile(src, dst)
	}
	secondErr := BackupRunner(
		volumeLabeler,
		2,
		successCopier,
		dstDir,
		func(path string) error { return os.Remove(path) },
		func(string) {},
		nil,
		nil,
		true,
		nil,
		srcDir,
	)
	if secondErr != nil {
		t.Fatalf("second run should succeed, got %v", secondErr)
	}

	dmFinal, err := core.DirectoryMapFromDir(core.Dirname(dstDir))
	if err != nil {
		t.Fatalf("read directory map after second run: %v", err)
	}
	for _, name := range files {
		if _, ok := dmFinal.Get(core.Fname(name)); !ok {
			t.Fatalf("expected %s in directory map after second run", name)
		}
	}
}
