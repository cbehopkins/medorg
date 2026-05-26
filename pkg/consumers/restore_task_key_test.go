package consumers

import (
	"testing"
	"time"
)

// TestRestoreContentKeyOrdering verifies that restoreContentKey orders by (MD5, Size).
func TestRestoreContentKeyOrdering(t *testing.T) {
	k1 := restoreContentKey{MD5: "aaaa", Size: 100}
	k2 := restoreContentKey{MD5: "aaaa", Size: 200}
	k3 := restoreContentKey{MD5: "bbbb", Size: 100}

	// k1 < k2 (same MD5; different size)
	if !restoreContentKeyLess(k1, k2) {
		t.Errorf("k1 should be less than k2")
	}

	// k1 < k3 (different MD5)
	if !restoreContentKeyLess(k1, k3) {
		t.Errorf("k1 should be less than k3")
	}

	// k2 is NOT less than k1 (reversed order)
	if restoreContentKeyLess(k2, k1) {
		t.Errorf("k2 should not be less than k1")
	}
}

// TestRestoreContentKeyEquals verifies equality check.
func TestRestoreContentKeyEquals(t *testing.T) {
	k1 := restoreContentKey{MD5: "aaaa", Size: 100}
	k2 := restoreContentKey{MD5: "aaaa", Size: 100}
	k3 := restoreContentKey{MD5: "aaaa", Size: 200}

	if !k1.Equals(k2) {
		t.Errorf("k1 and k2 should be equal")
	}

	if k1.Equals(k3) {
		t.Errorf("k1 and k3 should not be equal")
	}
}

// TestRestoreContentKeyMarshalUnmarshal verifies round-trip serialization.
func TestRestoreContentKeyMarshalUnmarshal(t *testing.T) {
	original := restoreContentKey{
		MD5:  "5d41402abc4b2a76b9719d911017c592",
		Size: 1234567890,
	}

	// Marshal
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal
	var restored restoreContentKey
	err = restored.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if !original.Equals(restored) {
		t.Errorf("Round-trip marshal failed: original %+v != restored %+v", original, restored)
	}
}

// TestRestoreTaskNodeMarshalUnmarshal verifies RestoreTaskNode payload round-trip.
func TestRestoreTaskNodeMarshalUnmarshal(t *testing.T) {
	now := time.Now().Unix()
	original := RestoreTaskNode{
		MD5:         "5d41402abc4b2a76b9719d911017c592",
		Size:        1234567890,
		BackupDests: []string{"BACKUP_VOL1", "BACKUP_VOL2"},
		Targets: []RestoreTaskTarget{
			{
				TaskID:        "5d41402abc4b2a76b9719d911017c592:1234567890:/restore/media/photos/vacation.jpg",
				Alias:         "photos",
				TargetAbsPath: "/restore/media/photos/vacation.jpg",
				CreatedAtUnix: now,
			},
		},
	}

	// Marshal
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal via interface
	payload, err := original.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	restored, ok := payload.(RestoreTaskNode)
	if !ok {
		t.Fatalf("Unmarshal returned wrong type: %T", payload)
	}

	if original.MD5 != restored.MD5 ||
		original.Size != restored.Size ||
		len(original.BackupDests) != len(restored.BackupDests) ||
		len(original.Targets) != len(restored.Targets) {
		t.Errorf("Round-trip marshal failed: original %+v != restored %+v", original, restored)
	}
}

// TestRestoreTaskNodeSizeInBytes verifies SizeInBytes returns reasonable size.
func TestRestoreTaskNodeSizeInBytes(t *testing.T) {
	node := RestoreTaskNode{
		MD5:         "5d41402abc4b2a76b9719d911017c592",
		Size:        1234567890,
		BackupDests: []string{"BACKUP_VOL1", "BACKUP_VOL2"},
		Targets: []RestoreTaskTarget{
			{
				TaskID:        "5d41402abc4b2a76b9719d911017c592:1234567890:/restore/media/photos/vacation.jpg",
				Alias:         "photos",
				TargetAbsPath: "/restore/media/photos/vacation.jpg",
			},
		},
	}

	size := node.SizeInBytes()
	if size <= 0 {
		t.Errorf("SizeInBytes returned non-positive value: %d", size)
	}

	// Verify it's at least roughly the JSON size
	if size < 100 {
		t.Errorf("SizeInBytes seems too small for this record: %d bytes", size)
	}
}

// TestNewRestoreTaskNode verifies the constructor copies backup destinations and seeds the target list.
func TestNewRestoreTaskNode(t *testing.T) {
	target := RestoreTaskTarget{TaskID: "id1", Alias: "alias", TargetAbsPath: "/restore/path"}
	backupDests := []string{"VOL1", "VOL2"}

	node := NewRestoreTaskNode(target, "hash", 42, backupDests)

	if node.MD5 != "hash" || node.Size != 42 {
		t.Fatalf("unexpected node identity: %+v", node)
	}
	if len(node.Targets) != 1 || node.Targets[0].TargetAbsPath != target.TargetAbsPath {
		t.Fatalf("unexpected targets in node: %+v", node.Targets)
	}
	if len(node.BackupDests) != 2 || node.BackupDests[0] != "VOL1" || node.BackupDests[1] != "VOL2" {
		t.Fatalf("unexpected backup destinations in node: %+v", node.BackupDests)
	}

	backupDests[0] = "CHANGED"
	if node.BackupDests[0] != "VOL1" {
		t.Fatalf("constructor should copy backup destinations, got %+v", node.BackupDests)
	}
}

// TestRestoreTaskNodeWithTarget verifies targets append and backup destinations merge without duplicates.
func TestRestoreTaskNodeWithTarget(t *testing.T) {
	base := RestoreTaskNode{
		MD5:         "hash",
		Size:        42,
		BackupDests: []string{"VOL1"},
		Targets: []RestoreTaskTarget{{
			TaskID:        "id1",
			Alias:         "alias1",
			TargetAbsPath: "/restore/one",
		}},
	}

	updated := base.WithTarget(RestoreTaskTarget{TaskID: "id2", Alias: "alias2", TargetAbsPath: "/restore/two"}, []string{"VOL1", "VOL2"})

	if len(updated.Targets) != 2 {
		t.Fatalf("expected 2 targets after merge, got %d", len(updated.Targets))
	}
	if updated.Targets[1].TargetAbsPath != "/restore/two" {
		t.Fatalf("unexpected appended target: %+v", updated.Targets[1])
	}
	if len(updated.BackupDests) != 2 || updated.BackupDests[0] != "VOL1" || updated.BackupDests[1] != "VOL2" {
		t.Fatalf("unexpected merged backup destinations: %+v", updated.BackupDests)
	}
}

// TestRestoreTaskNodeRemoveTarget verifies removal returns the updated node and removed target.
func TestRestoreTaskNodeRemoveTarget(t *testing.T) {
	base := RestoreTaskNode{
		MD5:  "hash",
		Size: 42,
		Targets: []RestoreTaskTarget{
			{TaskID: "id1", TargetAbsPath: "/restore/one"},
			{TaskID: "id2", TargetAbsPath: "/restore/two"},
		},
	}

	updated, removed, ok := base.RemoveTarget("/restore/one")
	if !ok {
		t.Fatalf("expected removal to succeed")
	}
	if removed.TaskID != "id1" {
		t.Fatalf("unexpected removed target: %+v", removed)
	}
	if len(updated.Targets) != 1 || updated.Targets[0].TaskID != "id2" {
		t.Fatalf("unexpected remaining targets: %+v", updated.Targets)
	}

	_, _, ok = updated.RemoveTarget("/restore/missing")
	if ok {
		t.Fatalf("expected missing target removal to fail")
	}
}

// TestNewRestoreContentKey verifies constructor produces correct key.
func TestNewRestoreContentKey(t *testing.T) {
	md5 := "5d41402abc4b2a76b9719d911017c592"
	size := int64(1234567890)

	key := NewRestoreContentKey(md5, size)

	if key.MD5 != md5 {
		t.Errorf("Key MD5 mismatch: expected %s, got %s", md5, key.MD5)
	}

	if key.Size != size {
		t.Errorf("Key Size mismatch: expected %d, got %d", size, key.Size)
	}
}
