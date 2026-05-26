package consumers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
	"github.com/cbehopkins/medorg/pkg/core"
)

// TestPriorityCollectionFromJSONL reproduces the priority collection behavior
// using a JSONL file captured from a real backup run.
// This test isolates the vault-backed treap operations to debug why
// many files are added but few are yielded by the iterator.
func TestPriorityCollectionFromJSONL(t *testing.T) {
	jsonlPath := createPriorityQueueJSONL(t, 10000)

	// Create a temporary vault for this test
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/test_priority.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Create priority collection exactly as the production code does
	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_priority",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create priority collection: %v", err)
	}

	// Also create a dst collection to simulate production scenario
	dstColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_dst",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create dst collection: %v", err)
	}

	// Read JSONL file and add entries to collection
	file, err := os.Open(jsonlPath)
	if err != nil {
		t.Fatalf("Failed to open JSONL file: %v", err)
	}
	defer file.Close()

	addedCount := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry struct {
			Timestamp          int64    `json:"timestamp"`
			Path               string   `json:"path"`
			Size               int64    `json:"size"`
			BackupDestinations []string `json:"backup_destinations"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("Failed to parse JSONL entry: %v", err)
		}

		// Reconstruct fileData
		fd := fileData{
			Size:       entry.Size,
			Fpath:      core.NewFpath(entry.Path),
			BackupDest: entry.BackupDestinations,
		}

		// Build priority key exactly as production code does
		key := buildPriorityKey(fd)

		// Insert into collection
		priorityColl.Insert(&key, fd)
		addedCount++

		if addedCount%10000 == 0 {
			log.Printf("Added %d files to priority collection...", addedCount)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error reading JSONL file: %v", err)
	}

	log.Printf("Total files added to priority collection: %d", addedCount)

	// Now iterate and count yielded files, simulating the production code
	// by adding files to dstColl as we "process" them
	yieldedCount := 0
	err = priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type: %T", node)
		}

		payload := payloadNode.GetPayload()
		yieldedCount++

		// Simulate adding to destination (as production code does)
		// This mimics bp.addDstFile() in the backup loop
		if yieldedCount%100 == 0 {
			dstKey := buildPriorityKey(payload)
			dstColl.Insert(&dstKey, payload)
		}

		// Log first few and periodic updates
		if yieldedCount <= 5 || yieldedCount%1000 == 0 {
			log.Printf("[Iter %d] File: %s (size: %d, dests: %v)",
				yieldedCount, payload.Fpath.String(), payload.Size, payload.BackupDest)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit error: %v", err)
	}

	log.Printf("Total files yielded by iterator: %d", yieldedCount)

	// This is the smoking gun - report the discrepancy
	if addedCount != yieldedCount {
		t.Errorf("DISCREPANCY FOUND: Added %d files but iterator yielded only %d files (%.2f%% loss)",
			addedCount, yieldedCount, 100.0*float64(addedCount-yieldedCount)/float64(addedCount))
	} else {
		t.Logf("SUCCESS: All %d files were correctly yielded", addedCount)
	}
}

// TestPriorityCollectionBasic tests basic priority collection operations
// with a small dataset to verify the mechanism works in simple cases
func TestPriorityCollectionBasic(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/test_priority_basic.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_priority_basic",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create priority collection: %v", err)
	}

	// Add a small number of test files
	testFiles := []fileData{
		{Size: 1000, Fpath: core.NewFpath("test", "file1.txt"), BackupDest: []string{}},
		{Size: 2000, Fpath: core.NewFpath("test", "file2.txt"), BackupDest: []string{"dest1"}},
		{Size: 500, Fpath: core.NewFpath("test", "file3.txt"), BackupDest: []string{}},
		{Size: 1500, Fpath: core.NewFpath("test", "file4.txt"), BackupDest: []string{"dest1", "dest2"}},
		{Size: 3000, Fpath: core.NewFpath("test", "file5.txt"), BackupDest: []string{}},
	}

	for _, fd := range testFiles {
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
	}

	// Iterate and collect results
	var results []fileData
	err = priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type: %T", node)
		}
		results = append(results, payloadNode.GetPayload())
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit error: %v", err)
	}

	// Verify we got all files
	if len(results) != len(testFiles) {
		t.Errorf("Expected %d files, got %d", len(testFiles), len(results))
	}

	// Verify ordering: fewest destinations first, then largest size
	// Expected order: file5 (0 dests, 3000), file1 (0 dests, 1000), file3 (0 dests, 500),
	//                 file2 (1 dest, 2000), file4 (2 dests, 1500)
	expectedOrder := []string{"file5.txt", "file1.txt", "file3.txt", "file2.txt", "file4.txt"}
	for i, fd := range results {
		if i < len(expectedOrder) && string(fd.Fpath.Base()) != expectedOrder[i] {
			t.Errorf("Result[%d]: expected %s, got %s", i, expectedOrder[i], string(fd.Fpath.Base()))
		}
		t.Logf("Result[%d]: %s (size: %d, dests: %d)",
			i, fd.Fpath.String(), fd.Size, len(fd.BackupDest))
	}
}

// TestPriorityCollectionDuplicateKeys tests what happens when we insert
// multiple items with the same priority key
func TestPriorityCollectionDuplicateKeys(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/test_priority_dups.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_priority_dups",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create priority collection: %v", err)
	}

	// Add files with identical priority keys (same size, same dest count, different paths)
	// The production code might be inserting many files with identical keys
	insertCount := 100
	for i := 0; i < insertCount; i++ {
		fd := fileData{
			Size:       1000, // Same size for all
			Fpath:      core.NewFpath("test", fmt.Sprintf("file%03d.txt", i)),
			BackupDest: []string{}, // Same dest count for all
		}
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
	}

	// Iterate and count
	yieldedCount := 0
	err = priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type: %T", node)
		}
		payload := payloadNode.GetPayload()
		yieldedCount++
		if yieldedCount <= 10 {
			t.Logf("Yielded[%d]: %s", yieldedCount, payload.Fpath.String())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit error: %v", err)
	}

	t.Logf("Inserted %d files, yielded %d files", insertCount, yieldedCount)

	// This might reveal the issue - does the treap collapse duplicate keys?
	if yieldedCount != insertCount {
		t.Errorf("ISSUE FOUND: Inserted %d files but only yielded %d files with duplicate keys",
			insertCount, yieldedCount)
	}
}
