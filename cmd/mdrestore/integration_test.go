package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

// TestFullBackupRestoreWorkflow is an end-to-end integration test that:
// 1. Creates test files in a source directory
// 2. Backs them up to multiple volumes using mdbackup
// 3. Creates a journal using mdjournal
// 4. Restores files using mdrestore (newdb + copy)
// 5. Verifies restored files match originals
func TestFullBackupRestoreWorkflow(t *testing.T) {
	// Create temporary test workspace
	tmpRoot, err := os.MkdirTemp("", "mdrestore-integration-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	// Setup directory structure
	sourceDir := filepath.Join(tmpRoot, "source")
	vol1Dir := filepath.Join(tmpRoot, "VOL1")
	vol2Dir := filepath.Join(tmpRoot, "VOL2")
	journalFile := filepath.Join(tmpRoot, "backup.journal.xml")
	restoreDir := filepath.Join(tmpRoot, "restored")
	dbFile := filepath.Join(tmpRoot, "restore.db")
	testConfigFile := filepath.Join(tmpRoot, "test.mdcfg.xml")

	for _, dir := range []string{sourceDir, vol1Dir, vol2Dir, restoreDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
	}

	// Create test files with known content
	testFiles := map[string]string{
		"photos/beach.jpg":     "beach photo content with some data",
		"photos/sunset.jpg":    "sunset photo with different content",
		"photos/family.jpg":    "family photo content here",
		"music/song1.mp3":      "music file 1 content",
		"music/song2.mp3":      "music file 2 content",
		"docs/report.pdf":      "pdf document content",
		"docs/notes.txt":       "text file with notes",
		"videos/clip1.mp4":     "video content 1",
		"videos/clip2.mp4":     "video content 2",
		"archive/old_data.zip": "archived data content",
	}

	t.Logf("Creating %d test files in %s", len(testFiles), sourceDir)
	for relPath, content := range testFiles {
		fullPath := filepath.Join(sourceDir, relPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("Failed to create parent dir for %s: %v", relPath, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write test file %s: %v", relPath, err)
		}
	}

	// Build required binaries if not already built
	binaries := []string{"mdcalc", "mdbackup", "mdjournal", "mdrestore", "mdlabel", "mdsource"}
	t.Log("Building required binaries...")
	for _, binary := range binaries {
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", filepath.Join(tmpRoot, binary+".exe"),
			"./cmd/"+binary)
		cmd.Dir = filepath.Join("..", "..")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Failed to build %s: %v\nOutput: %s", binary, err, output)
		}
	}

	mdcalcBin := filepath.Join(tmpRoot, "mdcalc.exe")
	mdbackupBin := filepath.Join(tmpRoot, "mdbackup.exe")
	mdjournalBin := filepath.Join(tmpRoot, "mdjournal.exe")
	mdrestoreBin := filepath.Join(tmpRoot, "mdrestore.exe")
	mdlabelBin := filepath.Join(tmpRoot, "mdlabel.exe")
	mdsourceBin := filepath.Join(tmpRoot, "mdsource.exe")

	// Step 1: Calculate checksums
	t.Log("Step 1: Calculating checksums with mdcalc...")
	cmd := exec.Command(mdcalcBin, sourceDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdcalc failed: %v\nOutput: %s", err, output)
	}
	t.Logf("mdcalc output: %s", output)

	// Verify .medorg.xml was created (check all subdirectories)
	medorgFiles := []string{}
	filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Name() == ".medorg.xml" {
			medorgFiles = append(medorgFiles, path)
		}
		return nil
	})

	if len(medorgFiles) == 0 {
		t.Fatalf(".medorg.xml not created in source directory or subdirectories")
	}
	t.Logf("Found %d .medorg.xml files", len(medorgFiles))

	backupVolumes := []struct {
		dir           string
		sourceSubdirs []string
	}{
		{dir: vol1Dir, sourceSubdirs: []string{"photos", "music"}},
		{dir: vol2Dir, sourceSubdirs: []string{"docs", "videos", "archive"}},
	}
	volumeForSourceSubdir := make(map[string]string)
	for _, volume := range backupVolumes {
		for _, sourceSubdir := range volume.sourceSubdirs {
			volumeForSourceSubdir[sourceSubdir] = volume.dir
		}
	}

	// Label volumes before backup (mdlabel generates random labels)
	t.Log("Labeling backup volumes...")
	for _, volume := range backupVolumes {
		// Create label using mdlabel
		cmd = exec.Command(mdlabelBin, "create", volume.dir)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("mdlabel create failed for %s: %v\nOutput: %s", filepath.Base(volume.dir), err, output)
		}
		// Read back the label using mdlabel show
		cmd = exec.Command(mdlabelBin, "show", volume.dir)
		labelOutput, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("Warning: Could not read label for %s: %v", filepath.Base(volume.dir), err)
		} else {
			if strings.TrimSpace(string(labelOutput)) == "" {
				t.Logf("Warning: Empty label output for %s", filepath.Base(volume.dir))
			} else {
				t.Logf("Created label for %s", filepath.Base(volume.dir))
			}
		}
	}

	// Step 2: Backup to multiple volumes
	for _, volume := range backupVolumes {
		t.Logf("Backing up to %s...", filepath.Base(volume.dir))
		args := []string{volume.dir}
		for _, sourceSubdir := range volume.sourceSubdirs {
			args = append(args, filepath.Join(sourceDir, sourceSubdir))
		}
		cmd = exec.Command(mdbackupBin, args...)
		cmd.Env = append(os.Environ(), "MEDORG_NO_PROGRESS=1", "NO_COLOR=1", "TERM=dumb")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("mdbackup %s failed: %v\nOutput: %s", filepath.Base(volume.dir), err, output)
		}
		t.Logf("mdbackup %s completed", filepath.Base(volume.dir))
	}

	for relPath := range testFiles {
		relDir := filepath.Dir(relPath)
		sourceFileDir := sourceDir
		if relDir != "." {
			sourceFileDir = filepath.Join(sourceDir, relDir)
		}
		sourceMap, err := core.DirectoryMapFromDir(core.Dirname(sourceFileDir), nil)
		if err != nil {
			t.Fatalf("Failed to read source metadata for %s: %v", relPath, err)
		}
		sourceFile, ok := sourceMap.Get(core.Fname(filepath.Base(relPath)))
		if !ok {
			t.Fatalf("Source metadata missing for %s", relPath)
		}
		if sourceFile.Checksum == "" || len(sourceFile.BackupDest) == 0 {
			t.Fatalf("Source metadata for %s is missing checksum or backup destinations: %+v", relPath, sourceFile)
		}

		volumeDir, ok := volumeForSourceSubdir[relDir]
		if !ok {
			t.Fatalf("No backup volume configured for source directory %s", relDir)
		}
		volumeMap, err := core.DirectoryMapFromDir(core.Dirname(volumeDir), nil)
		if err != nil {
			t.Fatalf("Failed to read volume metadata for %s on %s: %v", relPath, volumeDir, err)
		}
		volumeFile, ok := volumeMap.Get(core.Fname(filepath.Base(relPath)))
		if !ok || volumeFile.Checksum != sourceFile.Checksum {
			t.Fatalf("Backup metadata for %s does not match expected volume %s", relPath, filepath.Base(volumeDir))
		}
	}

	// Label the source directory before journaling so mdjournal recognizes it
	t.Log("Step 3a: Labeling source directory with mdlabel...")
	cmd = exec.Command(mdlabelBin, "create", sourceDir)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdlabel create on source failed: %v\nOutput: %s", err, output)
	}
	t.Log("Source directory labeled")

	// Add source directory as an alias in the config file
	t.Log("Step 3a2: Adding source alias with mdsource...")
	cmd = exec.Command(mdsourceBin, "add", "-config", testConfigFile, "-path", sourceDir, "-alias", "test")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdsource add failed: %v\nOutput: %s", err, output)
	}
	t.Log("Source alias added to config")

	// Configure restore destination using mdsource (tool-managed config)
	t.Log("Step 3a3: Configuring restore destination with mdsource...")
	cmd = exec.Command(mdsourceBin, "restore", "-config", testConfigFile, "-alias", "test", "-path", restoreDir)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdsource restore failed: %v\nOutput: %s", err, output)
	}
	t.Log("Restore destination configured")

	// Step 3b: Create journal
	t.Log("Step 3b: Creating journal with mdjournal...")
	cmd = exec.Command(mdjournalBin, "-config", testConfigFile, sourceDir)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdjournal failed: %v\nOutput: %s", err, output)
	}
	t.Logf("mdjournal output: %s", output)
	if !strings.Contains(string(output), testConfigFile) {
		t.Fatalf("mdjournal did not use explicit config path override; output: %s", output)
	}

	// mdjournal writes to ~/.mdjournal.xml, not the source directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("Failed to get home directory: %v", err)
	}
	homeJournalPath := filepath.Join(homeDir, ".mdjournal.xml")

	// Verify journal was created at home directory
	if _, err := os.Stat(homeJournalPath); os.IsNotExist(err) {
		t.Fatalf("Journal file not created at %s", homeJournalPath)
	}

	// Copy journal from home directory to test directory for later use
	journalData, err := os.ReadFile(homeJournalPath)
	if err != nil {
		t.Fatalf("Failed to read journal from %s: %v", homeJournalPath, err)
	}
	if err := os.WriteFile(journalFile, journalData, 0644); err != nil {
		t.Fatalf("Failed to copy journal to test dir: %v", err)
	}

	// Step 4: Create restore database from journal
	t.Log("Step 4: Creating restore database from journal...")
	cmd = exec.Command(mdrestoreBin, "newdb", "--config", testConfigFile, "--journal", journalFile, "--db", dbFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdrestore newdb failed: %v\nOutput: %s", err, output)
	}
	t.Logf("mdrestore newdb output: %s", output)

	// Parse output to verify targets were ingested
	outputStr := string(output)
	if !strings.Contains(outputStr, "Ingested") {
		t.Fatalf("Expected 'Ingested' in newdb output, got: %s", outputStr)
	}

	// Step 5: Restore from VOL1
	t.Log("Step 5a: Restoring from VOL1...")
	cmd = exec.Command(mdrestoreBin, "copy", "--config", testConfigFile, "--db", dbFile, vol1Dir)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdrestore copy VOL1 failed: %v\nOutput: %s", err, output)
	}
	t.Logf("mdrestore copy VOL1 output: %s", output)

	// VOL1 intentionally contains only part of the backup, so pending work must remain.
	expectedCopiedAfterVOL1 := 0
	for relPath := range testFiles {
		if volumeForSourceSubdir[filepath.Dir(relPath)] == vol1Dir {
			expectedCopiedAfterVOL1++
		}
	}
	pendingAfterVOL1, copiedAfterVOL1 := readRestoreCounts(t, dbFile)
	if pendingAfterVOL1 != len(testFiles)-expectedCopiedAfterVOL1 || copiedAfterVOL1 != expectedCopiedAfterVOL1 {
		t.Fatalf("Unexpected restore state after VOL1: pending=%d copied=%d; want pending=%d copied=%d",
			pendingAfterVOL1, copiedAfterVOL1, len(testFiles)-expectedCopiedAfterVOL1, expectedCopiedAfterVOL1)
	}

	// Step 5b: Restore from VOL2
	t.Log("Step 5b: Restoring from VOL2...")
	cmd = exec.Command(mdrestoreBin, "copy", "--config", testConfigFile, "--db", dbFile, vol2Dir)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdrestore copy VOL2 failed: %v\nOutput: %s", err, output)
	}
	t.Logf("mdrestore copy VOL2 output: %s", output)

	// Step 6: Compare original and restored directories
	t.Log("Step 6: Comparing original and restored files...")

	// The restore creates files under /restore/<alias>/ structure
	// Since we didn't use aliases, files should be under /restore/ subdirs
	// We need to walk the source and find corresponding files in restore

	// For now, let's check that at least some files were restored
	// and their content matches
	restoredCount := 0
	filepath.Walk(restoreDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		restoredCount++
		return nil
	})

	if restoredCount == 0 {
		t.Fatal("No files found in restore directory")
	}

	t.Logf("Found %d files in restore directory", restoredCount)

	// Verify content of restored files
	t.Log("Verifying restored file content...")
	verifiedCount := 0
	for relPath, originalContent := range testFiles {
		// Try to find the file in restore directory structure
		// It may be under various paths depending on alias mapping
		found := false
		filepath.Walk(restoreDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if filepath.Base(path) == filepath.Base(relPath) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Errorf("Failed to read restored file %s: %v", path, err)
					return nil
				}
				if string(content) == originalContent {
					verifiedCount++
					found = true
					t.Logf("✓ Verified: %s", relPath)
				} else {
					t.Errorf("Content mismatch for %s: expected %d bytes, got %d bytes",
						relPath, len(originalContent), len(content))
				}
			}
			return nil
		})
		if !found {
			t.Logf("Warning: Restored file not found for %s", relPath)
		}
	}

	t.Logf("Verified %d/%d files successfully restored with correct content", verifiedCount, len(testFiles))

	if verifiedCount != len(testFiles) {
		t.Fatalf("Verified %d/%d files; expected every file to be restored", verifiedCount, len(testFiles))
	}

	pendingCount, copiedCount := readRestoreCounts(t, dbFile)
	if pendingCount != 0 || copiedCount != len(testFiles) {
		t.Fatalf("Unexpected restore state: pending content=%d, copied content=%d; want 0 and %d", pendingCount, copiedCount, len(testFiles))
	}

	t.Log("Integration test completed successfully")
}

func readRestoreCounts(t *testing.T, dbPath string) (int, int) {
	t.Helper()

	db, err := consumers.OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open restore database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close restore database: %v", err)
		}
	})

	pendingCount, err := db.CountPending()
	if err != nil {
		t.Fatalf("Failed to count pending restore content: %v", err)
	}
	copiedCount, err := db.CountCopied()
	if err != nil {
		t.Fatalf("Failed to count copied restore content: %v", err)
	}
	return pendingCount, copiedCount
}
