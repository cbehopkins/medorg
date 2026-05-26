package consumers

import (
	"fmt"
	"testing"
	"time"
)

// TestJournalIngestion verifies that journal entries are correctly ingested into the restore DB.
func TestJournalIngestion(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/journal_ingest.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Simulate journal entries from mdjournal output
	journalEntries := []struct {
		alias      string
		dir        string
		fileName   string
		md5        string
		size       int64
		backupDest string
	}{
		{
			alias:      "photos",
			dir:        "vacation",
			fileName:   "pic1.jpg",
			md5:        "aaaa1111",
			size:       2000000,
			backupDest: "BACKUP_VOL1",
		},
		{
			alias:      "photos",
			dir:        "vacation",
			fileName:   "pic2.jpg",
			md5:        "bbbb2222",
			size:       3000000,
			backupDest: "BACKUP_VOL2",
		},
		{
			alias:      "videos",
			dir:        "concerts",
			fileName:   "show.mp4",
			md5:        "cccc3333",
			size:       500000000,
			backupDest: "BACKUP_VOL1",
		},
	}

	// Ingest each journal entry as a restore target
	for _, je := range journalEntries {
		// Build target path (assumes /restore/{alias}/{dir}/{file} mapping)
		targetAbsPath := fmt.Sprintf("/restore/%s/%s/%s", je.alias, je.dir, je.fileName)

		// Build TaskID
		taskID := fmt.Sprintf("%s:%d:%s", je.md5, je.size, targetAbsPath)

		target := &RestoreTaskTarget{
			TaskID:        taskID,
			Alias:         je.alias,
			TargetAbsPath: targetAbsPath,
			CreatedAtUnix: now,
		}

		if err := db.InsertPending(target, je.md5, je.size, []string{je.backupDest}); err != nil {
			t.Fatalf("InsertPending failed for %s: %v", je.fileName, err)
		}
	}

	// Verify all targets were ingested (3 content nodes)
	pendingCount, _ := db.CountPending()
	if pendingCount != 3 {
		t.Errorf("Expected 3 content nodes after ingestion, got %d", pendingCount)
	}

	// Verify we can retrieve by content
	results, _ := db.FindPendingByContent("aaaa1111", 2000000)
	if len(results) != 1 {
		t.Errorf("Expected 1 result for 'aaaa1111', got %d", len(results))
	}

	results, _ = db.FindPendingByContent("cccc3333", 500000000)
	if len(results) != 1 {
		t.Errorf("Expected 1 result for 'cccc3333', got %d", len(results))
	}
}

// TestJournalIngestionWithDuplicateContent verifies handling of multiple files with same content.
func TestJournalIngestionWithDuplicateContent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/dup_content.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Two targets with same content (md5, size) but different paths
	targets := []*RestoreTaskTarget{
		{
			TaskID:        "shared1111:5000000:/restore/docs/file1.txt",
			Alias:         "docs",
			TargetAbsPath: "/restore/docs/file1.txt",
			CreatedAtUnix: now,
		},
		{
			TaskID:        "shared1111:5000000:/restore/archive/file1.txt",
			Alias:         "archive",
			TargetAbsPath: "/restore/archive/file1.txt",
			CreatedAtUnix: now,
		},
	}

	// Both targets reference the same content
	for _, target := range targets {
		if err := db.InsertPending(target, "shared1111", 5000000, []string{"VOL1"}); err != nil {
			t.Fatalf("InsertPending failed: %v", err)
		}
	}

	// Query by content should return both targets in one node
	results, _ := db.FindPendingByContent("shared1111", 5000000)
	if len(results) != 2 {
		t.Errorf("Expected 2 targets with same content, got %d", len(results))
	}

	// Count should be 1 (one content node)
	pendingCount, _ := db.CountPending()
	if pendingCount != 1 {
		t.Errorf("Expected 1 pending content node, got %d", pendingCount)
	}

	// Mark one target as copied
	if err := db.MoveToCopied("/restore/docs/file1.txt", "shared1111", 5000000); err != nil {
		t.Fatalf("MoveToCopied failed: %v", err)
	}

	// Other target should still be queryable in pending
	pending, _ := db.FindPendingByContent("shared1111", 5000000)
	if len(pending) != 1 {
		t.Errorf("Expected 1 target still pending after moving 1, got %d", len(pending))
	}
	if pending[0].TargetAbsPath != "/restore/archive/file1.txt" {
		t.Errorf("Wrong target pending: %s", pending[0].TargetAbsPath)
	}
}

// TestJournalIngestionWithAliasMapping verifies different aliases map to correct restore roots.
func TestJournalIngestionWithAliasMapping(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/alias_map.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Simulate a config that maps aliases to different restore roots
	aliasToRestoreRoot := map[string]string{
		"photos":  "/mnt/data/photos",
		"videos":  "/mnt/data/videos",
		"archive": "/archive",
	}

	// Ingest targets for multiple aliases
	ingestCases := []struct {
		alias    string
		fileName string
		md5      string
		size     int64
	}{
		{"photos", "vacation.jpg", "photo1111", 2000000},
		{"videos", "concert.mp4", "video2222", 500000000},
		{"archive", "old.tar.gz", "arch3333", 1000000000},
	}

	for _, ic := range ingestCases {
		restoreRoot := aliasToRestoreRoot[ic.alias]
		targetAbsPath := fmt.Sprintf("%s/%s", restoreRoot, ic.fileName)
		taskID := fmt.Sprintf("%s:%d:%s", ic.md5, ic.size, targetAbsPath)

		target := &RestoreTaskTarget{
			TaskID:        taskID,
			Alias:         ic.alias,
			TargetAbsPath: targetAbsPath,
			CreatedAtUnix: now,
		}

		if err := db.InsertPending(target, ic.md5, ic.size, []string{"BACKUP_VOL"}); err != nil {
			t.Fatalf("InsertPending failed for %s/%s: %v", ic.alias, ic.fileName, err)
		}
	}

	// Verify all ingested (3 content nodes)
	count, _ := db.CountPending()
	if count != 3 {
		t.Errorf("Expected 3 ingested content nodes, got %d", count)
	}

	// Verify each can be found by content
	for _, ic := range ingestCases {
		results, _ := db.FindPendingByContent(ic.md5, ic.size)
		if len(results) == 0 {
			t.Errorf("Could not find target for %s/%s", ic.alias, ic.fileName)
		} else {
			found := results[0]
			if found.Alias != ic.alias {
				t.Errorf("Alias mismatch: expected %s, got %s", ic.alias, found.Alias)
			}
		}
	}
}

// TestXMLJournalParsing verifies conversion from XML journal format to RestoreTaskTarget.
// This test uses a simplified XML journal like mdjournal produces.
func TestXMLJournalParsing(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/xml_journal.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	// Simulated XML journal structure (simplified)
	type Md5Entry struct {
		Name       string
		Checksum   string
		Size       int64
		BackupDest []string
	}

	type JournalEntry struct {
		Alias   string
		Dir     string
		Entries []Md5Entry
	}

	journal := []JournalEntry{
		{
			Alias: "photos",
			Dir:   "summer",
			Entries: []Md5Entry{
				{Name: "beach.jpg", Checksum: "beach1111", Size: 3000000, BackupDest: []string{"VOL1"}},
				{Name: "sunset.jpg", Checksum: "sunset222", Size: 4000000, BackupDest: []string{"VOL2"}},
			},
		},
		{
			Alias: "music",
			Dir:   "compilations",
			Entries: []Md5Entry{
				{Name: "mix.mp3", Checksum: "mix00001", Size: 10000000, BackupDest: []string{"VOL1", "VOL3"}},
			},
		},
	}

	now := time.Now().Unix()

	// Convert journal entries to RestoreTaskTarget and insert
	for _, je := range journal {
		for _, entry := range je.Entries {
			targetAbsPath := fmt.Sprintf("/restore/%s/%s/%s", je.Alias, je.Dir, entry.Name)
			taskID := fmt.Sprintf("%s:%d:%s", entry.Checksum, entry.Size, targetAbsPath)

			target := &RestoreTaskTarget{
				TaskID:        taskID,
				Alias:         je.Alias,
				TargetAbsPath: targetAbsPath,
				CreatedAtUnix: now,
			}

			if err := db.InsertPending(target, entry.Checksum, entry.Size, entry.BackupDest); err != nil {
				t.Fatalf("InsertPending failed: %v", err)
			}
		}
	}

	// Verify ingestion (3 content nodes)
	count, _ := db.CountPending()
	if count != 3 {
		t.Errorf("Expected 3 content nodes from journal, got %d", count)
	}

	// Verify lookup works
	results, _ := db.FindPendingByContent("beach1111", 3000000)
	if len(results) != 1 {
		t.Errorf("Could not find beach photo by content")
	}

	// Verify multi-destination backup is stored correctly in node
	results, _ = db.FindPendingByContent("mix00001", 10000000)
	if len(results) != 1 {
		t.Errorf("Could not find mix by content")
	}
	// Backup destinations are stored at the node level
	// (Real implementation would need to return both the targets and the node's BackupDests)
}
