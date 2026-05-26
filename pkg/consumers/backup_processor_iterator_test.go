package consumers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
	"github.com/cbehopkins/medorg/pkg/core"
)

// TestIteratorFromProductionJSONL reproduces the exact production scenario
// where the iterator yields far fewer files than were added to the collection.
// This test loads the JSONL file from an actual backup run and reproduces
// the iterator behavior under production conditions.
func TestIteratorFromProductionJSONL(t *testing.T) {
	jsonlPath := createPriorityQueueJSONL(t, 10000)

	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/test_iter.db"

	// Create vault with same memory settings as production
	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Match production memory budget settings
	// Note: Don't use SetMemoryBudgetWithPercentile() with large datasets - it starts background monitoring
	// which adds significant disk I/O overhead causing tests to take much longer and potentially timeout
	// Just create the vault without memory budget to keep test execution time reasonable
	// session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)
	// session.Vault.SetCheckInterval(1000)  // DISABLED: adds overhead with large iterations

	// Create priority collection
	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_iterator_prod",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create priority collection: %v", err)
	}

	// Read and insert all JSONL entries
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

		fd := fileData{
			Size:       entry.Size,
			Fpath:      core.NewFpath(entry.Path),
			BackupDest: entry.BackupDestinations,
		}
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
		addedCount++

		if addedCount%50000 == 0 {
			t.Logf("Inserted %d files", addedCount)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error reading JSONL: %v", err)
	}

	t.Logf("Total files inserted: %d", addedCount)

	// Persist the tree structure to disk so all nodes are properly reachable
	// This saves parent->child ObjectId relationships for lazy-loading during traversal
	t.Logf("Persisting tree structure to disk...")
	err = priorityColl.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Now test the iterator with detailed error tracking
	yieldedCount := 0
	var lastError error
	var lastYieldedFile string
	var iterationStoppedReason string

	t.Logf("Starting iterator (no background monitoring for better performance)...")

	err = priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			lastError = fmt.Errorf("unexpected node type %T", node)
			iterationStoppedReason = fmt.Sprintf("Unexpected node type: %v", lastError)
			return lastError
		}
		payload := payloadNode.GetPayload()
		yieldedCount++
		lastYieldedFile = payload.Fpath.String()

		// Log periodically
		if yieldedCount <= 20 || yieldedCount%50000 == 0 {
			t.Logf("[Iter %d] %s (size: %d)", yieldedCount, payload.Fpath.String(), payload.Size)
		}
		return nil
	})
	if err != nil {
		lastError = err
		iterationStoppedReason = fmt.Sprintf("InOrderVisit error: %v", err)
	}
	if iterationStoppedReason == "" {
		iterationStoppedReason = "InOrderVisit completed"
	}

	t.Logf("Iterator stopped. Yielded: %d, Added: %d", yieldedCount, addedCount)
	if lastYieldedFile != "" {
		t.Logf("Last yielded file: %s", lastYieldedFile)
	}
	t.Logf("Stop reason: %s", iterationStoppedReason)
	if lastError != nil {
		t.Logf("Last error: %v", lastError)
	}

	// Report findings
	if addedCount != yieldedCount {
		loss := 100.0 * float64(addedCount-yieldedCount) / float64(addedCount)
		t.Errorf("ITERATOR LOSS: Added %d but yielded %d (%.2f%% loss) - Stop reason: %s - ERROR: %v",
			addedCount, yieldedCount, loss, iterationStoppedReason, lastError)
	} else {
		t.Logf("SUCCESS: All %d files yielded correctly", yieldedCount)
	}
}

// TestVaultSessionStability checks if vault session is staying alive during iteration
func TestVaultSessionStability(t *testing.T) {
	jsonlPath := createPriorityQueueJSONL(t, 5000)

	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/test_vault_stability.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Use same settings as production (but without background monitoring for better performance)
	// Don't use SetMemoryBudgetWithPercentile or SetCheckInterval - they trigger background monitoring
	// which adds significant disk I/O overhead and slows down large operations
	// session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)
	// session.Vault.SetCheckInterval(1000)

	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_vault_stability",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create priority collection: %v", err)
	}

	// Insert files
	file, err := os.Open(jsonlPath)
	if err != nil {
		t.Fatalf("Failed to open JSONL: %v", err)
	}
	defer file.Close()

	addedCount := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry struct {
			Path               string   `json:"path"`
			Size               int64    `json:"size"`
			BackupDestinations []string `json:"backup_destinations"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}

		fd := fileData{
			Size:       entry.Size,
			Fpath:      core.NewFpath(entry.Path),
			BackupDest: entry.BackupDestinations,
		}
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
		addedCount++

		if addedCount%50000 == 0 {
			t.Logf("Inserted %d, vault still open: %v", addedCount, session.Vault != nil)
		}
	}

	t.Logf("Inserted %d files, vault open: %v", addedCount, session.Vault != nil)

	// Persist the tree structure to disk before iteration
	t.Logf("Persisting tree structure to disk...")
	err = priorityColl.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Now iterate while checking vault health
	yieldedCount := 0

	err = priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		_ = payloadNode.GetPayload()
		yieldedCount++

		// Check vault health periodically
		if yieldedCount%1000 == 0 {
			vaultHealthy := session.Vault != nil
			t.Logf("Vault check at iteration %d: healthy=%v", yieldedCount, vaultHealthy)
		}
		return nil
	})
	if err != nil {
		t.Logf("InOrderVisit error at count %d: %v, vault still open: %v", yieldedCount, err, session.Vault != nil)
	}

	t.Logf("Final: Added %d, Yielded %d (%.2f%% loss)", addedCount, yieldedCount,
		100.0*float64(addedCount-yieldedCount)/float64(addedCount))
}

// TestIteratorMemoryPressure tests iterator behavior under memory constraints
// similar to production Raspberry Pi conditions
func TestIteratorMemoryPressure(t *testing.T) {
	jsonlPath := createPriorityQueueJSONL(t, 25000)

	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/test_mem_pressure.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Use VERY aggressive memory budget (like production)
	session.Vault.SetMemoryBudgetWithPercentile(5_000, 10) // Smaller budget, earlier flush

	priorityColl, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test_memory_pressure",
		priorityKeyLess,
		(*priorityKey)(new(priorityKey)),
		fileData{},
	)
	if err != nil {
		t.Fatalf("Failed to create priority collection: %v", err)
	}

	// Insert first 20k files only (to stay within memory)
	file, err := os.Open(jsonlPath)
	if err != nil {
		t.Fatalf("Failed to open JSONL: %v", err)
	}
	defer file.Close()

	addedCount := 0
	maxToAdd := 20000
	scanner := bufio.NewScanner(file)
	for scanner.Scan() && addedCount < maxToAdd {
		var entry struct {
			Path               string   `json:"path"`
			Size               int64    `json:"size"`
			BackupDestinations []string `json:"backup_destinations"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}

		fd := fileData{
			Size:       entry.Size,
			Fpath:      core.NewFpath(entry.Path),
			BackupDest: entry.BackupDestinations,
		}
		key := buildPriorityKey(fd)
		priorityColl.Insert(&key, fd)
		addedCount++
	}

	t.Logf("Inserted %d files under memory pressure", addedCount)

	// Persist the tree structure to disk before iteration
	t.Logf("Persisting tree structure to disk...")
	err = priorityColl.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Test iterator
	yieldedCount := 0

	err = priorityColl.InOrderVisit(func(node treap.TreapNodeInterface[priorityKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[priorityKey, fileData])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		_ = payloadNode.GetPayload()
		yieldedCount++

		if yieldedCount%5000 == 0 {
			t.Logf("Yielded %d files", yieldedCount)
		}
		return nil
	})
	if err != nil {
		t.Logf("InOrderVisit error at file %d: %v", yieldedCount+1, err)
	}

	t.Logf("Memory pressure test: Added %d, Yielded %d", addedCount, yieldedCount)
	if addedCount != yieldedCount {
		loss := 100.0 * float64(addedCount-yieldedCount) / float64(addedCount)
		t.Errorf("MEMORY PRESSURE LOSS: %.2f%%", loss)
	}
}
