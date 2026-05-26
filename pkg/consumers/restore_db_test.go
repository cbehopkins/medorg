package consumers

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRestoreDBCreateAndClose verifies basic open/close lifecycle.
func TestRestoreDBCreateAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/test.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify DB file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("DB file not created at %s", dbPath)
	}
}

// TestRestoreDBInsertAndRetrievePending verifies insert and retrieval of pending targets.
func TestRestoreDBInsertAndRetrievePending(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/test.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	target := &RestoreTaskTarget{
		TaskID:        "abc123:1000:/target/file1",
		Alias:         "media",
		TargetAbsPath: "/target/file1",
		CreatedAtUnix: now,
	}

	// Insert into pending
	if err := db.InsertPending(target, "abc123", 1000, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertPending failed: %v", err)
	}

	// Retrieve by content (md5, size)
	results, err := db.FindPendingByContent("abc123", 1000)
	if err != nil {
		t.Fatalf("FindPendingByContent failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].TaskID != target.TaskID {
		t.Errorf("TaskID mismatch: expected %s, got %s", target.TaskID, results[0].TaskID)
	}
}

// TestRestoreDBMoveToCopied verifies transition from pending to copied collection.
func TestRestoreDBMoveToCopied(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/test.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	target := &RestoreTaskTarget{
		TaskID:        "def456:2000:/target/file2",
		Alias:         "photos",
		TargetAbsPath: "/target/file2",
		CreatedAtUnix: now,
	}

	// Insert into pending
	if err := db.InsertPending(target, "def456", 2000, []string{"VOL2"}); err != nil {
		t.Fatalf("InsertPending failed: %v", err)
	}

	// Verify it's in pending
	pending, err := db.FindPendingByContent("def456", 2000)
	if err != nil {
		t.Fatalf("FindPendingByContent failed: %v", err)
	}
	if len(pending) != 1 {
		t.Errorf("Expected task in pending before move")
	}

	// Move to copied
	if err := db.MoveToCopied("/target/file2", "def456", 2000); err != nil {
		t.Fatalf("MoveToCopied failed: %v", err)
	}

	// Verify it's no longer in pending
	pending, _ = db.FindPendingByContent("def456", 2000)
	if len(pending) != 0 {
		t.Errorf("Expected task removed from pending after move, but found %d", len(pending))
	}

	// Verify it's in copied (via count)
	copiedCount, err := db.CountCopied()
	if err != nil {
		t.Fatalf("CountCopied failed: %v", err)
	}
	if copiedCount != 1 {
		t.Errorf("Expected 1 task in copied, got %d", copiedCount)
	}
}

// TestRestoreDBMultipleTasks verifies handling multiple targets with same content.
func TestRestoreDBMultipleTasks(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/test.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Two targets with same content (md5, size) but different paths
	target1 := &RestoreTaskTarget{
		TaskID:        "xyz789:3000:/target/a",
		Alias:         "media",
		TargetAbsPath: "/target/a",
		CreatedAtUnix: now,
	}

	target2 := &RestoreTaskTarget{
		TaskID:        "xyz789:3000:/target/b",
		Alias:         "media",
		TargetAbsPath: "/target/b",
		CreatedAtUnix: now,
	}

	if err := db.InsertPending(target1, "xyz789", 3000, []string{"VOL1", "VOL2"}); err != nil {
		t.Fatalf("InsertPending target1 failed: %v", err)
	}
	if err := db.InsertPending(target2, "xyz789", 3000, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertPending target2 failed: %v", err)
	}

	// Retrieve both by content
	results, err := db.FindPendingByContent("xyz789", 3000)
	if err != nil {
		t.Fatalf("FindPendingByContent failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results for same content, got %d", len(results))
	}

	// Move one to copied, verify the other remains
	if err := db.MoveToCopied("/target/a", "xyz789", 3000); err != nil {
		t.Fatalf("MoveToCopied failed: %v", err)
	}

	remaining, _ := db.FindPendingByContent("xyz789", 3000)
	if len(remaining) != 1 {
		t.Errorf("Expected 1 target remaining in pending after moving 1, got %d", len(remaining))
	}
	if remaining[0].TargetAbsPath != "/target/b" {
		t.Errorf("Wrong target remained: expected /target/b, got %s", remaining[0].TargetAbsPath)
	}
}

// TestRestoreDBPersistenceAcrossReopen verifies data survives close and reopen.
func TestRestoreDBPersistenceAcrossReopen(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/persist.db", tmpDir)

	now := time.Now().Unix()
	target := &RestoreTaskTarget{
		TaskID:        "persist123:5000:/target/persist",
		Alias:         "backup",
		TargetAbsPath: "/target/persist",
		CreatedAtUnix: now,
	}

	// Session 1: Insert and close
	{
		db, err := OpenRestoreDB(dbPath)
		if err != nil {
			t.Fatalf("OpenRestoreDB session1 failed: %v", err)
		}

		if err := db.InsertPending(target, "persist123", 5000, []string{"BACKUP_VOL"}); err != nil {
			t.Fatalf("InsertPending failed: %v", err)
		}

		count, err := db.CountPending()
		if err != nil || count != 1 {
			t.Fatalf("CountPending session1 failed or returned wrong value: %v, %d", err, count)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("Close session1 failed: %v", err)
		}
	}

	// Session 2: Reopen and verify data is still there
	{
		db, err := OpenRestoreDB(dbPath)
		if err != nil {
			t.Fatalf("OpenRestoreDB session2 failed: %v", err)
		}
		defer db.Close()

		count, err := db.CountPending()
		if err != nil {
			t.Fatalf("CountPending session2 failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 content node in pending after reopen, got %d", count)
		}

		// Retrieve the target
		results, err := db.FindPendingByContent("persist123", 5000)
		if err != nil {
			t.Fatalf("FindPendingByContent session2 failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result after reopen, got %d", len(results))
		} else if results[0].TaskID != target.TaskID {
			t.Errorf("TaskID mismatch after reopen: expected %s, got %s", target.TaskID, results[0].TaskID)
		}

		// Move to copied and ensure the copied collection is persisted too.
		if err := db.MoveToCopied(target.TargetAbsPath, "persist123", 5000); err != nil {
			t.Fatalf("MoveToCopied session2 failed: %v", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("Close session2 failed: %v", err)
		}
	}

	// Session 3: Reopen again and verify copied state persists.
	{
		db, err := OpenRestoreDB(dbPath)
		if err != nil {
			t.Fatalf("OpenRestoreDB session3 failed: %v", err)
		}
		defer db.Close()

		pendingCount, err := db.CountPending()
		if err != nil {
			t.Fatalf("CountPending session3 failed: %v", err)
		}
		if pendingCount != 0 {
			t.Errorf("Expected 0 content nodes in pending after move and reopen, got %d", pendingCount)
		}

		copiedCount, err := db.CountCopied()
		if err != nil {
			t.Fatalf("CountCopied session3 failed: %v", err)
		}
		if copiedCount != 1 {
			t.Errorf("Expected 1 content node in copied after move and reopen, got %d", copiedCount)
		}
	}
}

// TestRestoreDBCountPendingCopied verifies count operations.
func TestRestoreDBCountPendingCopied(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/count.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()

	// Start empty
	pendingCount, _ := db.CountPending()
	copiedCount, _ := db.CountCopied()
	if pendingCount != 0 || copiedCount != 0 {
		t.Errorf("Expected empty DB, got pending=%d, copied=%d", pendingCount, copiedCount)
	}

	// Insert target (single content node)
	target := &RestoreTaskTarget{
		TaskID:        "count1:1000:/t1",
		Alias:         "a",
		TargetAbsPath: "/t1",
		CreatedAtUnix: now,
	}
	db.InsertPending(target, "count1", 1000, []string{"V1"})

	// Verify counts (one content node with one target)
	pendingCount, _ = db.CountPending()
	copiedCount, _ = db.CountCopied()
	if pendingCount != 1 || copiedCount != 0 {
		t.Errorf("After insert: expected pending=1 node, copied=0; got pending=%d, copied=%d", pendingCount, copiedCount)
	}

	// Move to copied
	db.MoveToCopied("/t1", "count1", 1000)

	// Verify counts changed (node moved, counts = pending content nodes, copied content nodes)
	pendingCount, _ = db.CountPending()
	copiedCount, _ = db.CountCopied()
	if pendingCount != 0 || copiedCount != 1 {
		t.Errorf("After move: expected pending=0, copied=1; got pending=%d, copied=%d", pendingCount, copiedCount)
	}
}

// TestRestoreDBLookupByContentSemantics verifies that lookup keys are exact on checksum and size.
func TestRestoreDBLookupByContentSemantics(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/lookup.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	target := &RestoreTaskTarget{
		TaskID:        "sharedhash:1234:/restore/example.txt",
		Alias:         "example",
		TargetAbsPath: "/restore/example.txt",
		CreatedAtUnix: now,
	}

	if err := db.InsertPending(target, "sharedhash", 1234, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertPending failed: %v", err)
	}

	matches, err := db.FindPendingByContent("sharedhash", 1234)
	if err != nil {
		t.Fatalf("FindPendingByContent exact lookup failed: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected exact lookup to return 1 match, got %d", len(matches))
	}

	missBySize, err := db.FindPendingByContent("sharedhash", 1235)
	if err != nil {
		t.Fatalf("FindPendingByContent size mismatch lookup failed: %v", err)
	}
	if len(missBySize) != 0 {
		t.Fatalf("Expected size mismatch lookup to return 0 matches, got %d", len(missBySize))
	}

	missByHash, err := db.FindPendingByContent("otherhash", 1234)
	if err != nil {
		t.Fatalf("FindPendingByContent hash mismatch lookup failed: %v", err)
	}
	if len(missByHash) != 0 {
		t.Fatalf("Expected hash mismatch lookup to return 0 matches, got %d", len(missByHash))
	}
}

// TestRestoreDBCloseIdempotent verifies that closing twice is safe.
func TestRestoreDBCloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/close.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be idempotent, got: %v", err)
	}
}

// TestRestoreDBOperationAfterClose verifies operations reject once the DB is shut down.
func TestRestoreDBOperationAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/shutdown.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	target := &RestoreTaskTarget{TaskID: "id", Alias: "alias", TargetAbsPath: "/restore/path"}
	if err := db.InsertPending(target, "hash", 1, []string{"VOL1"}); err == nil {
		t.Fatalf("expected InsertPending to fail after close")
	}
	if _, err := db.FindPendingByContent("hash", 1); err == nil {
		t.Fatalf("expected FindPendingByContent to fail after close")
	}
}

// TestRestoreDBInsertCopied verifies the copied collection stores and merges targets correctly.
func TestRestoreDBInsertCopied(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/copied.db", tmpDir)

	db, err := OpenRestoreDB(dbPath)
	if err != nil {
		t.Fatalf("OpenRestoreDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	target1 := &RestoreTaskTarget{TaskID: "hash:7:/restore/one", Alias: "one", TargetAbsPath: "/restore/one", CreatedAtUnix: now}
	target2 := &RestoreTaskTarget{TaskID: "hash:7:/restore/two", Alias: "two", TargetAbsPath: "/restore/two", CreatedAtUnix: now}

	if err := db.InsertCopied(target1, "hash", 7, []string{"VOL1"}); err != nil {
		t.Fatalf("InsertCopied target1 failed: %v", err)
	}
	if err := db.InsertCopied(target2, "hash", 7, []string{"VOL1", "VOL2"}); err != nil {
		t.Fatalf("InsertCopied target2 failed: %v", err)
	}

	copiedCount, err := db.CountCopied()
	if err != nil {
		t.Fatalf("CountCopied failed: %v", err)
	}
	if copiedCount != 1 {
		t.Fatalf("expected 1 copied content node, got %d", copiedCount)
	}

	results, err := db.FindPendingByContent("hash", 7)
	if err != nil {
		t.Fatalf("FindPendingByContent failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected pending to remain empty for copied-only inserts, got %d", len(results))
	}

	key := NewRestoreContentKey("hash", 7)
	payloadNode := db.copiedColl.Search(&key)
	if payloadNode == nil {
		t.Fatalf("expected copied node to exist")
	}
	payload, ok := payloadNode.(interface{ GetPayload() RestoreTaskNode })
	if !ok {
		t.Fatalf("unexpected copied node type: %T", payloadNode)
	}
	node := payload.GetPayload()
	if len(node.Targets) != 2 {
		t.Fatalf("expected 2 targets in copied node, got %d", len(node.Targets))
	}
	if len(node.BackupDests) != 2 || node.BackupDests[0] != "VOL1" || node.BackupDests[1] != "VOL2" {
		t.Fatalf("expected merged copied destinations, got %+v", node.BackupDests)
	}
}

func TestWithRestoreDBRunsAndCloses(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/with.db", tmpDir)

	var captured *RestoreDB
	err := WithRestoreDB(dbPath, func(db *RestoreDB) error {
		captured = db
		target := &RestoreTaskTarget{TaskID: "hash:1:/restore/a", Alias: "a", TargetAbsPath: "/restore/a"}
		return db.InsertPending(target, "hash", 1, []string{"VOL1"})
	})
	if err != nil {
		t.Fatalf("WithRestoreDB failed: %v", err)
	}

	if captured == nil {
		t.Fatalf("expected callback to receive db")
	}

	if _, err := captured.CountPending(); err == nil {
		t.Fatalf("expected DB operations to fail after helper close")
	}
}

func TestWithRestoreDBPropagatesCallbackError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := fmt.Sprintf("%s/with-error.db", tmpDir)

	expectedErr := fmt.Errorf("callback boom")
	err := WithRestoreDB(dbPath, func(db *RestoreDB) error {
		return expectedErr
	})
	if err == nil {
		t.Fatalf("expected callback error")
	}
	if err.Error() != expectedErr.Error() {
		t.Fatalf("expected callback error %q, got %q", expectedErr.Error(), err.Error())
	}
}
