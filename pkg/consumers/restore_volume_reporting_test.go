package consumers

import (
	"path/filepath"
	"testing"
)

// TestCountPendingByBackupDest verifies that CountPendingByBackupDest accurately groups pending targets by volume.
func TestCountPendingByBackupDest(t *testing.T) {
	tmpDB := filepath.Join(t.TempDir(), "test_count_pending_by_dest.db")

	db, err := OpenRestoreDB(tmpDB)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Insert targets with different backup destinations
	targets := []struct {
		alias       string
		path        string
		md5         string
		size        int64
		backupDests []string
	}{
		{"photos", "/restore/photos/beach.jpg", "abc123", 3000000, []string{"VOL1"}},
		{"photos", "/restore/photos/sunset.jpg", "def456", 4000000, []string{"VOL2"}},
		{"photos", "/restore/photos/family.jpg", "ghi789", 2500000, []string{"VOL1"}},
		{"music", "/restore/music/mix1.mp3", "mno345", 10000000, []string{"VOL1"}},
		{"music", "/restore/music/mix2.mp3", "pqr678", 12000000, []string{"VOL2"}},
		{"docs", "/restore/docs/report.pdf", "jkl012", 3500000, []string{"VOL3"}},
		// File available on multiple volumes
		{"shared", "/restore/shared/common.dat", "stu901", 5000000, []string{"VOL1", "VOL2"}},
	}

	for _, tc := range targets {
		target := &RestoreTaskTarget{
			TaskID:        tc.md5 + ":" + string(rune(tc.size)) + ":" + tc.path,
			Alias:         tc.alias,
			TargetAbsPath: tc.path,
		}
		if err := db.InsertPending(target, tc.md5, tc.size, tc.backupDests); err != nil {
			t.Fatalf("Failed to insert target %s: %v", tc.path, err)
		}
	}

	// Count pending by backup destination
	counts, err := db.CountPendingByBackupDest()
	if err != nil {
		t.Fatalf("CountPendingByBackupDest failed: %v", err)
	}

	// Expected counts (note: common.dat appears in both VOL1 and VOL2)
	expected := map[string]int{
		"VOL1": 4, // beach.jpg, family.jpg, mix1.mp3, common.dat
		"VOL2": 3, // sunset.jpg, mix2.mp3, common.dat
		"VOL3": 1, // report.pdf
	}

	if len(counts) != len(expected) {
		t.Errorf("Expected %d volumes, got %d: %v", len(expected), len(counts), counts)
	}

	for vol, expectedCount := range expected {
		if counts[vol] != expectedCount {
			t.Errorf("Volume %s: expected %d files, got %d", vol, expectedCount, counts[vol])
		}
	}
}

// TestCountPendingByBackupDestAfterMove verifies that counts update correctly after moving targets to copied.
func TestCountPendingByBackupDestAfterMove(t *testing.T) {
	tmpDB := filepath.Join(t.TempDir(), "test_count_after_move.db")

	db, err := OpenRestoreDB(tmpDB)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Insert 3 targets on VOL1, each with proper structure
	paths := []string{"/restore/photo1.jpg", "/restore/photo2.jpg", "/restore/photo3.jpg"}
	md5s := []string{"md51", "md52", "md53"}
	sizes := []int64{1000, 2000, 3000}

	for i := 0; i < 3; i++ {
		target := &RestoreTaskTarget{
			TaskID:        md5s[i] + ":size:" + paths[i],
			Alias:         "photos",
			TargetAbsPath: paths[i],
		}
		if err := db.InsertPending(target, md5s[i], sizes[i], []string{"VOL1"}); err != nil {
			t.Fatalf("Failed to insert target %d: %v", i, err)
		}
	}

	// Initial count
	counts, err := db.CountPendingByBackupDest()
	if err != nil {
		t.Fatalf("CountPendingByBackupDest failed: %v", err)
	}
	if counts["VOL1"] != 3 {
		t.Errorf("Expected 3 files on VOL1, got %d", counts["VOL1"])
	}

	// Move one target to copied
	if err := db.MoveToCopied(paths[0], md5s[0], sizes[0]); err != nil {
		t.Fatalf("MoveToCopied failed: %v", err)
	}

	// Count should now be 2
	counts, err = db.CountPendingByBackupDest()
	if err != nil {
		t.Fatalf("CountPendingByBackupDest failed after move: %v", err)
	}
	if counts["VOL1"] != 2 {
		t.Errorf("Expected 2 files on VOL1 after move, got %d", counts["VOL1"])
	}

	// Move remaining targets
	db.MoveToCopied(paths[1], md5s[1], sizes[1])
	db.MoveToCopied(paths[2], md5s[2], sizes[2])

	// Count should now be 0 (or empty map)
	counts, err = db.CountPendingByBackupDest()
	if err != nil {
		t.Fatalf("CountPendingByBackupDest failed after all moved: %v", err)
	}
	if counts["VOL1"] != 0 && len(counts) > 0 {
		t.Errorf("Expected 0 files on VOL1 after all moved, got %d", counts["VOL1"])
	}
}
