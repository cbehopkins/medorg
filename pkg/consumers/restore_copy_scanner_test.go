package consumers

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
)

func checksumForTest(t *testing.T, filePath string) string {
	t.Helper()
	hash, err := core.CalcMd5File(filepath.Dir(filePath), filepath.Base(filePath))
	if err != nil {
		t.Fatalf("CalcMd5File failed for %s: %v", filePath, err)
	}
	return hash
}

// TestScanSourceForPendingBasic verifies scanning a source with matching files.
func TestScanSourceForPendingBasic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create restore DB
	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Create a pending task for content we'll put in the source
	target1 := &RestoreTaskTarget{
		TaskID:        "file1hash:1000:/restore/photos/pic1.jpg",
		Alias:         "photos",
		TargetAbsPath: "/restore/photos/pic1.jpg",
		CreatedAtUnix: now,
	}
	if err := db.InsertPending(target1, "file1hash", 1000, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertPending failed: %v", err)
	}

	// Create source directory with a file matching that content
	sourceDir := filepath.Join(tmpDir, "source")
	os.MkdirAll(sourceDir, 0755)

	// Create a test file with known content (for reproducible hash)
	testFile := filepath.Join(sourceDir, "pic1.jpg")
	testContent := []byte("photo data content 1000 bytes")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Note: In real usage, file size would match (1000 bytes), but our test uses
	// actual file size. Compute the actual hash.
	actualHash := checksumForTest(t, testFile)

	// Update the DB with the actual hash
	db.InsertPending(target1, actualHash, int64(len(testContent)), []string{"VOL1"})

	// Scan the source
	plan, err := ScanSourceForPending(sourceDir, db)
	if err != nil {
		t.Fatalf("ScanSourceForPending failed: %v", err)
	}

	// Verify the plan
	if len(plan.Operations) != 1 {
		t.Errorf("Expected 1 copy operation, got %d", len(plan.Operations))
	}

	if plan.MatchedCount != 1 {
		t.Errorf("Expected 1 matched file, got %d", plan.MatchedCount)
	}

	if plan.UnmatchedCount != 0 {
		t.Errorf("Expected 0 unmatched files, got %d", plan.UnmatchedCount)
	}

	if len(plan.Operations) > 0 {
		op := plan.Operations[0]
		if op.DestinationPath != "/restore/photos/pic1.jpg" {
			t.Errorf("Destination path mismatch: expected /restore/photos/pic1.jpg, got %s", op.DestinationPath)
		}
		if op.Alias != "photos" {
			t.Errorf("Alias mismatch: expected photos, got %s", op.Alias)
		}
	}
}

// TestScanSourceForPendingMultipleTargets verifies a single source file matched to multiple targets.
func TestScanSourceForPendingMultipleTargets(t *testing.T) {
	tmpDir := t.TempDir()

	// Create restore DB
	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Create a test file
	sourceDir := filepath.Join(tmpDir, "source")
	os.MkdirAll(sourceDir, 0755)

	testFile := filepath.Join(sourceDir, "shared.dat")
	testContent := []byte("shared file content")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Compute its hash
	hash := checksumForTest(t, testFile)
	size := int64(len(testContent))

	// Create two pending targets for the same content
	target1 := &RestoreTaskTarget{
		TaskID:        hash + ":1:/restore/docs/copy1.txt",
		Alias:         "docs",
		TargetAbsPath: "/restore/docs/copy1.txt",
		CreatedAtUnix: now,
	}

	target2 := &RestoreTaskTarget{
		TaskID:        hash + ":1:/restore/archive/copy2.txt",
		Alias:         "archive",
		TargetAbsPath: "/restore/archive/copy2.txt",
		CreatedAtUnix: now,
	}

	if err := db.InsertPending(target1, hash, size, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertPending target1 failed: %v", err)
	}

	if err := db.InsertPending(target2, hash, size, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertPending target2 failed: %v", err)
	}

	// Scan the source
	plan, err := ScanSourceForPending(sourceDir, db)
	if err != nil {
		t.Fatalf("ScanSourceForPending failed: %v", err)
	}

	// Should have 2 copy operations (one file, two destinations)
	if len(plan.Operations) != 2 {
		t.Errorf("Expected 2 copy operations, got %d", len(plan.Operations))
	}

	if plan.MatchedCount != 1 {
		t.Errorf("Expected 1 matched file, got %d", plan.MatchedCount)
	}

	// Verify both operations reference the same source
	if len(plan.Operations) >= 2 {
		if plan.Operations[0].SourcePath != plan.Operations[1].SourcePath {
			t.Errorf("Both operations should have same source path")
		}
		if plan.Operations[0].DestinationPath == plan.Operations[1].DestinationPath {
			t.Errorf("Operations should have different destination paths")
		}
	}
}

// TestScanSourceForPendingMixed verifies scanning with both matched and unmatched files.
func TestScanSourceForPendingMixed(t *testing.T) {
	tmpDir := t.TempDir()

	// Create restore DB
	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Create source directory with multiple files
	sourceDir := filepath.Join(tmpDir, "source")
	os.MkdirAll(sourceDir, 0755)

	// File 1: Will match a pending task
	file1 := filepath.Join(sourceDir, "match.txt")
	content1 := []byte("matched file")
	os.WriteFile(file1, content1, 0644)
	hash1 := checksumForTest(t, file1)

	// File 2: Will NOT match (no pending task)
	file2 := filepath.Join(sourceDir, "unmatched.txt")
	content2 := []byte("unmatched file content")
	os.WriteFile(file2, content2, 0644)

	// Add file1's content to pending
	target := &RestoreTaskTarget{
		TaskID:        hash1 + ":1:/restore/match.txt",
		Alias:         "test",
		TargetAbsPath: "/restore/match.txt",
		CreatedAtUnix: now,
	}
	db.InsertPending(target, hash1, int64(len(content1)), []string{"VOL1"})

	// Scan
	plan, err := ScanSourceForPending(sourceDir, db)
	if err != nil {
		t.Fatalf("ScanSourceForPending failed: %v", err)
	}

	// Should find 1 matched, 1 unmatched
	if plan.MatchedCount != 1 {
		t.Errorf("Expected 1 matched, got %d", plan.MatchedCount)
	}

	if plan.UnmatchedCount != 1 {
		t.Errorf("Expected 1 unmatched, got %d", plan.UnmatchedCount)
	}

	if len(plan.Operations) != 1 {
		t.Errorf("Expected 1 copy operation, got %d", len(plan.Operations))
	}
}

// TestScanSourceForPendingNonExistent verifies error handling for missing source dir.
func TestScanSourceForPendingNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	// Try to scan a directory that doesn't exist
	nonExistentDir := filepath.Join(tmpDir, "does_not_exist")

	plan, err := ScanSourceForPending(nonExistentDir, db)

	if err == nil {
		t.Errorf("Expected error for non-existent directory, got nil")
	}

	if plan != nil {
		t.Errorf("Expected nil plan for error case, got %+v", plan)
	}
}

// TestScanSourceForPendingNested verifies scanning nested directory structures.
func TestScanSourceForPendingNested(t *testing.T) {
	tmpDir := t.TempDir()

	// Create restore DB
	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Create nested source structure
	sourceDir := filepath.Join(tmpDir, "source")
	subDir := filepath.Join(sourceDir, "subdir", "nested")
	os.MkdirAll(subDir, 0755)

	// Create a file in the nested directory
	nestedFile := filepath.Join(subDir, "deep.txt")
	content := []byte("deeply nested file")
	os.WriteFile(nestedFile, content, 0644)
	hash := checksumForTest(t, nestedFile)

	// Add pending task for it
	target := &RestoreTaskTarget{
		TaskID:        hash + ":1:/restore/deep.txt",
		Alias:         "nested",
		TargetAbsPath: "/restore/deep.txt",
		CreatedAtUnix: now,
	}
	db.InsertPending(target, hash, int64(len(content)), []string{"VOL1"})

	// Scan
	plan, err := ScanSourceForPending(sourceDir, db)
	if err != nil {
		t.Fatalf("ScanSourceForPending failed: %v", err)
	}

	if len(plan.Operations) != 1 {
		t.Errorf("Expected 1 operation from nested file, got %d", len(plan.Operations))
	}

	if plan.Operations[0].SourcePath != nestedFile {
		t.Errorf("Expected source path %s, got %s", nestedFile, plan.Operations[0].SourcePath)
	}
}

// TestCopyPlanStats verifies plan statistics calculations.
func TestCopyPlanStats(t *testing.T) {
	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Create source with 5 files: 2 matched, 3 unmatched
	sourceDir := filepath.Join(tmpDir, "source")
	os.MkdirAll(sourceDir, 0755)

	for i := 1; i <= 5; i++ {
		file := filepath.Join(sourceDir, fmt.Sprintf("file%d.txt", i))
		content := []byte(fmt.Sprintf("content %d", i))
		os.WriteFile(file, content, 0644)

		// Add pending tasks only for files 1 and 2
		if i <= 2 {
			hash := checksumForTest(t, file)
			target := &RestoreTaskTarget{
				TaskID:        fmt.Sprintf("%s:1:/restore/file%d.txt", hash, i),
				Alias:         "test",
				TargetAbsPath: fmt.Sprintf("/restore/file%d.txt", i),
				CreatedAtUnix: now,
			}
			db.InsertPending(target, hash, int64(len(content)), []string{"VOL1"})
		}
	}

	plan, err := ScanSourceForPending(sourceDir, db)
	if err != nil {
		t.Fatalf("ScanSourceForPending failed: %v", err)
	}

	if plan.MatchedCount != 2 {
		t.Errorf("Expected 2 matched files, got %d", plan.MatchedCount)
	}

	if plan.UnmatchedCount != 3 {
		t.Errorf("Expected 3 unmatched files, got %d", plan.UnmatchedCount)
	}

	if len(plan.Operations) != 2 {
		t.Errorf("Expected 2 copy operations, got %d", len(plan.Operations))
	}

	if plan.ErrorCount != 0 {
		t.Errorf("Expected 0 errors, got %d", plan.ErrorCount)
	}
}

// TestScanSourceForPendingDeterministicSet verifies repeated scans produce the same operation set.
func TestScanSourceForPendingDeterministicSet(t *testing.T) {
	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "restore.db")
	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	files := []struct {
		name string
		data []byte
	}{
		{name: "a.bin", data: []byte("alpha")},
		{name: "b.bin", data: []byte("bravo")},
	}

	for _, f := range files {
		path := filepath.Join(sourceDir, f.name)
		if err := os.WriteFile(path, f.data, 0o644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
		hash := checksumForTest(t, path)
		size := int64(len(f.data))

		t1 := &RestoreTaskTarget{
			TaskID:        hash + ":1:/restore/" + f.name,
			Alias:         "det",
			TargetAbsPath: "/restore/" + f.name,
			CreatedAtUnix: now,
		}
		if err := db.InsertPending(t1, hash, size, []string{"VOL1"}); err != nil {
			t.Fatalf("InsertPending failed: %v", err)
		}

		// First file has an additional restore target to verify multi-target stability.
		if f.name == "a.bin" {
			t2 := &RestoreTaskTarget{
				TaskID:        hash + ":2:/restore/dup-" + f.name,
				Alias:         "det",
				TargetAbsPath: "/restore/dup-" + f.name,
				CreatedAtUnix: now,
			}
			if err := db.InsertPending(t2, hash, size, []string{"VOL1"}); err != nil {
				t.Fatalf("InsertPending duplicate target failed: %v", err)
			}
		}
	}

	plans := make([]*CopyPlan, 0, 3)
	for i := 0; i < 3; i++ {
		plan, err := ScanSourceForPending(sourceDir, db)
		if err != nil {
			t.Fatalf("ScanSourceForPending run %d failed: %v", i+1, err)
		}
		plans = append(plans, plan)
	}

	base := plans[0]
	baseSet := make(map[string]struct{}, len(base.Operations))
	for _, op := range base.Operations {
		key := op.SourcePath + "|" + op.DestinationPath + "|" + op.MD5 + "|" + fmt.Sprintf("%d", op.Size)
		baseSet[key] = struct{}{}
	}

	for i := 1; i < len(plans); i++ {
		p := plans[i]
		if p.MatchedCount != base.MatchedCount || p.UnmatchedCount != base.UnmatchedCount || p.ErrorCount != base.ErrorCount {
			t.Fatalf("plan stats changed between runs: base=(%d,%d,%d) run%d=(%d,%d,%d)", base.MatchedCount, base.UnmatchedCount, base.ErrorCount, i+1, p.MatchedCount, p.UnmatchedCount, p.ErrorCount)
		}
		if len(p.Operations) != len(base.Operations) {
			t.Fatalf("operation count changed between runs: base=%d run%d=%d", len(base.Operations), i+1, len(p.Operations))
		}

		seen := make(map[string]struct{}, len(p.Operations))
		for _, op := range p.Operations {
			key := op.SourcePath + "|" + op.DestinationPath + "|" + op.MD5 + "|" + fmt.Sprintf("%d", op.Size)
			seen[key] = struct{}{}
		}

		if len(seen) != len(baseSet) {
			t.Fatalf("operation set cardinality changed between runs: base=%d run%d=%d", len(baseSet), i+1, len(seen))
		}

		for key := range baseSet {
			if _, ok := seen[key]; !ok {
				t.Fatalf("missing operation in run %d: %s", i+1, key)
			}
		}
	}
}
