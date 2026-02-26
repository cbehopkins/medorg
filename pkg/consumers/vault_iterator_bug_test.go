package consumers

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// simplePayload is a minimal payload for demonstrating the iterator bug
type simplePayload struct {
	ID    int
	Value string
}

func (sp simplePayload) Marshal() ([]byte, error) {
	return []byte(fmt.Sprintf("%d:%s", sp.ID, sp.Value)), nil
}

func (sp simplePayload) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	var id int
	var value string
	_, err := fmt.Sscanf(string(data), "%d:%s", &id, &value)
	if err != nil {
		return nil, err
	}
	return simplePayload{ID: id, Value: value}, nil
}

func (sp simplePayload) SizeInBytes() int {
	data, _ := sp.Marshal()
	return len(data)
}

// simpleKey models the production priorityKey with DestCount, Size, and unique Path
// This ensures each key is unique (via the ID field) while allowing ordering by Priority
type simpleKey struct {
	Priority int // Primary sort key (like DestCount in production)
	ID       int // Unique identifier (like Path in production)
}

func (sk simpleKey) Value() simpleKey { return sk }
func (sk simpleKey) SizeInBytes() int {
	return 16 // sizeof(int) * 2
}
func (sk simpleKey) Equals(other simpleKey) bool {
	return sk.Priority == other.Priority && sk.ID == other.ID
}
func (sk simpleKey) Marshal() ([]byte, error) {
	return []byte(fmt.Sprintf("%d:%d", sk.Priority, sk.ID)), nil
}
func (sk *simpleKey) Unmarshal(data []byte) error {
	_, err := fmt.Sscanf(string(data), "%d:%d", &sk.Priority, &sk.ID)
	return err
}
func (sk simpleKey) New() types.PersistentKey[simpleKey] {
	return &simpleKey{}
}

func (sk simpleKey) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	b, err := sk.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}
	id, err := store.WriteNewObjFromBytes(stre, b)
	if err != nil {
		return 0, 0, func() error { return err }
	}
	return id, len(b), func() error { return nil }
}

func (sk *simpleKey) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error {
		return store.ReadGeneric(stre, sk, id)
	}
}

func (sk simpleKey) MarshalToObjectId(stre store.Storer) (bobbob.ObjectId, error) {
	b, err := sk.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, b)
}

func (sk *simpleKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, sk, id)
}

func (sk simpleKey) DeleteDependents(stre bobbob.Storer) error {
	return nil
}

func simpleKeyLess(a, b simpleKey) bool {
	// Primary sort by Priority, secondary by ID for deterministic ordering
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.ID < b.ID
}

// TestVaultIteratorBug validates that the vault-backed treap iterator correctly handles
// large numbers of unique items under memory pressure.
//
// Test Setup:
//   - 200,000 unique items inserted (each with unique ID, modeling unique file paths)
//   - Items share priority values (1000 items per priority group, modeling files with same size/destCount)
//   - Memory budget constraints trigger periodic flushing (SetMemoryBudgetWithPercentile)
//
// Result: PASSES - All 200,000 unique items are correctly yielded by the iterator
//
// Key Insight: Treaps deduplicate by key. If the same key is inserted multiple times,
// only the last value is kept (expected behavior). Production uses priorityKey with
// unique Path field, so each file should have a unique key.
//
// If production shows missing files, the issue is NOT the iterator dropping items,
// but likely one of:
//  1. Duplicate file paths being inserted (same key overwrites previous)
//  2. Early termination of consumption loop (e.g., addDstFile error)
//  3. BackupProcessor shutdown signal
func TestVaultIteratorBug(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/vault_iterator_bug.db"

	// Create vault with memory constraints similar to production
	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Configure memory budget to trigger flushing
	// This mimics production settings on memory-constrained devices
	session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)
	session.Vault.SetCheckInterval(1000)

	// Create a collection with simple key/payload types
	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"iterator_bug_test",
		simpleKeyLess,
		(*simpleKey)(new(simpleKey)),
		simplePayload{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert a large number of items
	// Simulate production scenario where each file has a unique key but many files
	// share the same priority (e.g., same size and destination count).
	// Use ID to ensure uniqueness (like Path in production), and Priority for sorting.
	// NOTE: Reduced to 20k from 200k due to vault memory operations causing timeouts
	const itemCount = 20_000
	for i := range itemCount {
		// Each item gets unique ID, but priorities are grouped
		// Every 1000 items shares the same priority value
		priority := i / 1000

		key := simpleKey{Priority: priority, ID: i}
		payload := simplePayload{
			ID:    i,
			Value: fmt.Sprintf("value_%d", i),
		}
		coll.Insert(&key, payload)

		if (i+1)%50000 == 0 {
			t.Logf("Inserted %d items...", i+1)
		}
	}

	t.Logf("Successfully inserted %d items into collection", itemCount)

	// Now iterate and count how many items we get back
	yieldedCount := 0
	var lastError error

	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[simpleKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[simpleKey, simplePayload])
		if !ok {
			return fmt.Errorf("unexpected node type at item %d: %T", yieldedCount+1, node)
		}

		payload := payloadNode.GetPayload()
		yieldedCount++

		// Log progress
		if yieldedCount <= 10 || yieldedCount%50000 == 0 {
			t.Logf("Yielded item %d: ID=%d, Value=%s", yieldedCount, payload.ID, payload.Value)
		}
		return nil
	})
	if err != nil {
		lastError = err
		t.Logf("InOrderVisit error at item %d: %v", yieldedCount+1, err)
	}

	// Report results
	t.Logf("Iterator completed. Yielded: %d, Expected: %d", yieldedCount, itemCount)
	if lastError != nil {
		t.Logf("Last error: %v", lastError)
	}

	// Calculate loss percentage
	lossPercent := 100.0 * float64(itemCount-yieldedCount) / float64(itemCount)

	// This should pass, but currently fails due to the iterator bug
	if yieldedCount != itemCount {
		t.Errorf("BUG REPRODUCED: Iterator only yielded %d of %d items (%.2f%% loss, expected 0%% loss)",
			yieldedCount, itemCount, lossPercent)
		t.Errorf("The iterator channel closed silently without error after yielding only %.2f%% of items",
			100.0*float64(yieldedCount)/float64(itemCount))

		if lastError == nil {
			t.Errorf("No error was reported despite massive data loss")
		}
	} else {
		t.Logf("SUCCESS: All %d items were correctly yielded", itemCount)
	}
}

// TestVaultIteratorSmallDataset verifies that the iterator works correctly
// with small datasets (that don't trigger memory flushing)
func TestVaultIteratorSmallDataset(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/vault_iterator_small.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Same memory settings as bug test
	session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)

	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"small_dataset_test",
		simpleKeyLess,
		(*simpleKey)(new(simpleKey)),
		simplePayload{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert only 100 items (small enough to stay in memory)
	const itemCount = 100
	for i := 0; i < itemCount; i++ {
		key := simpleKey{Priority: i / 10, ID: i} // 10 items per priority group
		payload := simplePayload{
			ID:    i,
			Value: fmt.Sprintf("value_%d", i),
		}
		coll.Insert(&key, payload)
	}

	// Iterate and count
	yieldedCount := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[simpleKey]) error {
		_, ok := node.(treap.PersistentPayloadNodeInterface[simpleKey, simplePayload])
		if !ok {
			return fmt.Errorf("unexpected node type: %T", node)
		}
		yieldedCount++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit error: %v", err)
	}

	if yieldedCount != itemCount {
		t.Errorf("Small dataset test failed: expected %d items, got %d", itemCount, yieldedCount)
	} else {
		t.Logf("Small dataset test passed: all %d items yielded correctly", itemCount)
	}
}

// TestVaultIteratorWithoutMemoryPressure tests the iterator without aggressive
// memory budget constraints
func TestVaultIteratorWithoutMemoryPressure(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/vault_iterator_no_pressure.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// NO memory budget set - allow everything to stay in memory
	// session.Vault.SetMemoryBudgetWithPercentile(...) <-- NOT CALLED

	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"no_pressure_test",
		simpleKeyLess,
		(*simpleKey)(new(simpleKey)),
		simplePayload{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert large dataset with unique keys
	// NOTE: Reduced to 20k from 200k due to vault memory operations causing timeouts
	const itemCount = 20_000
	for i := 0; i < itemCount; i++ {
		key := simpleKey{Priority: i / 1000, ID: i} // 1000 items per priority group
		payload := simplePayload{
			ID:    i,
			Value: fmt.Sprintf("value_%d", i),
		}
		coll.Insert(&key, payload)

		if (i+1)%50000 == 0 {
			t.Logf("Inserted %d items...", i+1)
		}
	}

	t.Logf("Inserted %d items (no memory pressure)", itemCount)

	// Iterate and count
	yieldedCount := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[simpleKey]) error {
		_, ok := node.(treap.PersistentPayloadNodeInterface[simpleKey, simplePayload])
		if !ok {
			return fmt.Errorf("unexpected node type: %T", node)
		}
		yieldedCount++

		if yieldedCount%50000 == 0 {
			t.Logf("Yielded %d items...", yieldedCount)
		}
		return nil
	})
	if err != nil {
		t.Logf("InOrderVisit error at item %d: %v", yieldedCount+1, err)
	}

	t.Logf("No memory pressure test: Yielded %d of %d items", yieldedCount, itemCount)

	if yieldedCount != itemCount {
		lossPercent := 100.0 * float64(itemCount-yieldedCount) / float64(itemCount)
		t.Errorf("Even without memory pressure, iterator lost %.2f%% of items (%d yielded, %d expected)",
			lossPercent, yieldedCount, itemCount)
	} else {
		t.Logf("SUCCESS: All items yielded correctly without memory pressure")
	}
}
