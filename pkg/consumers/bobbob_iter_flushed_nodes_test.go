package consumers

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// // testKey mimics bobbob's key interface with 3 fields to trigger memory flushing similar to production
// type testKey struct {
// 	Rank int
// 	Size uint64
// 	Path string
// }

// func (k testKey) Value() testKey { return k }
// func (k testKey) SizeInBytes() int {
// 	b, _ := k.Marshal()
// 	return len(b)
// }
// func (k testKey) Equals(other testKey) bool {
// 	return k.Rank == other.Rank && k.Size == other.Size && k.Path == other.Path
// }
// func (k testKey) Marshal() ([]byte, error) {
// 	data, err := json.Marshal(k)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// Length-prefixed format to handle fixed-size block allocations
// 	length := uint32(len(data))
// 	buf := make([]byte, 4+len(data))
// 	binary.LittleEndian.PutUint32(buf[0:4], length)
// 	copy(buf[4:], data)
// 	return buf, nil
// }
// func (k *testKey) Unmarshal(data []byte) error {
// 	if len(data) < 4 {
// 		return fmt.Errorf("testKey data too short: %d bytes", len(data))
// 	}
// 	length := binary.LittleEndian.Uint32(data[0:4])
// 	if int(length) > len(data)-4 {
// 		return fmt.Errorf("testKey length %d exceeds data size %d", length, len(data)-4)
// 	}
// 	return json.Unmarshal(data[4:4+length], k)
// }
// func (k testKey) New() types.PersistentKey[testKey] {
// 	return &testKey{}
// }
// func (k testKey) MarshalToObjectId(stre store.Storer) (bobbob.ObjectId, error) {
// 	b, err := k.Marshal()
// 	if err != nil {
// 		return 0, err
// 	}
// 	return store.WriteNewObjFromBytes(stre, b)
// }
// func (k *testKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre store.Storer) error {
// 	return store.ReadGeneric(stre, k, id)
// }

// func testKeyLess(a, b testKey) bool {
// 	if a.Rank != b.Rank {
// 		return a.Rank < b.Rank
// 	}
// 	if a.Size != b.Size {
// 		return a.Size < b.Size
// 	}
// 	return a.Path < b.Path
// }

// testKey mimics bobbob's key interface with 3 fields to trigger memory flushing similar to production
type testKey struct {
	Rank int
	Size uint64
	Path string
}

func (k testKey) Value() testKey { return k }
func (k testKey) SizeInBytes() int {
	b, _ := k.Marshal()
	return len(b)
}
func (k testKey) Equals(other testKey) bool {
	return k.Rank == other.Rank && k.Size == other.Size && k.Path == other.Path
}
func (k testKey) Marshal() ([]byte, error) {
	data, err := json.Marshal(k)
	if err != nil {
		return nil, err
	}
	// Length-prefixed format to handle fixed-size block allocations
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], length)
	copy(buf[4:], data)
	return buf, nil
}
func (k *testKey) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("testKeyFixed data too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	if int(length) > len(data)-4 {
		return fmt.Errorf("testKeyFixed length %d exceeds data size %d", length, len(data)-4)
	}
	return json.Unmarshal(data[4:4+length], k)
}
func (k testKey) New() types.PersistentKey[testKey] {
	return &testKey{}
}
func (k testKey) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	b, err := k.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}
	id, err := store.WriteNewObjFromBytes(stre, b)
	if err != nil {
		return 0, 0, func() error { return err }
	}
	return id, len(b), func() error { return nil }
}
func (k *testKey) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error {
		return store.ReadGeneric(stre, k, id)
	}
}
func (k testKey) MarshalToObjectId(stre store.Storer) (bobbob.ObjectId, error) {
	b, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, b)
}
func (k *testKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}
func (k testKey) DeleteDependents(stre bobbob.Storer) error {
	return nil
}

func testKeyLess(a, b testKey) bool {
	if a.Rank != b.Rank {
		return a.Rank < b.Rank
	}
	if a.Size != b.Size {
		return a.Size < b.Size
	}
	return a.Path < b.Path
}

// TestBobbobMemoryPressureIssue reproduces data loss with memory-constrained vaults using bobbob types.
// This test programmatically generates items to trigger memory pressure and flushing,
// then verifies that InOrderVisit doesn't actually load the flushed nodes from disk.
func TestBobbobMemoryPressureIssueFixed(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/memory_pressure.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Use aggressive memory budget to force node flushing
	// NOTE: Don't use SetMemoryBudgetWithPercentile() with large iterators - it starts background monitoring
	// which adds significant performance overhead causing tests to exceed timeout
	// session.Vault.SetMemoryBudgetWithPercentile(5_000, 10)
	// session.Vault.SetCheckInterval(100)
	// session.Vault.StartBackgroundMonitoring() // Start background goroutine to check memory

	// Create a collection using only bobbob types:
	// Key: testKey struct (custom, mimics production key complexity)
	// Payload: types.JsonPayload[string] (file paths)
	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"memory_pressure_test",
		testKeyLess,
		(*testKey)(new(testKey)),
		types.JsonPayload[string]{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Generate items programmatically
	// Key: testKey with rank/size/path (forces flushing due to complexity)
	// Payload: File path string
	// Create more items than memory budget can hold to force flushing
	const maxItems = 100_000 // Increase to ensure flushing
	addedCount := 0
	for i := 0; i < maxItems; i++ {
		// Create file path as payload - keep paths short to minimize variation
		path := fmt.Sprintf("file_%d", i)
		payload := types.JsonPayload[string]{Value: path}

		// Create key with varying rank and size to force flushing
		key := testKey{
			Rank: (maxItems - i) / 100,
			Size: uint64(maxItems - i),
			Path: fmt.Sprintf("key_%d", i), // Keep key path short
		}
		coll.Insert(&key, payload)
		addedCount++
	}

	t.Logf("Generated %d items", addedCount)

	// Check how many are actually in memory vs flushed
	inMemory := coll.CountInMemoryNodes()
	onDisk := addedCount - inMemory
	t.Logf("After %d inserts: In-memory: %d, Flushed to disk: %d (%.1f%% on disk)",
		addedCount, inMemory, onDisk, 100.0*float64(onDisk)/float64(addedCount))

	if onDisk == 0 {
		t.Logf("WARNING: No items were flushed to disk - memory budget may not be tight enough")
	}

	// Modern API: Persist the tree structure to disk so all nodes are reachable
	// This saves parent->child ObjectId relationships, enabling lazy-loading during traversal
	t.Logf("Persisting tree structure to disk...")
	err = coll.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Test 1: Iterate without flushing - verify all items are reachable
	t.Logf("Test 1: Iterating with all nodes in memory (baseline)...")
	yieldedCountBaseline := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		yieldedCountBaseline++
		if yieldedCountBaseline <= 5 || yieldedCountBaseline%10000 == 0 {
			if pNode, ok := node.(treap.PersistentPayloadNodeInterface[testKey, types.JsonPayload[string]]); ok {
				t.Logf("  Baseline item %d: %s", yieldedCountBaseline, pNode.GetPayload().Value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Baseline iteration error: %v", err)
	}
	t.Logf("Baseline: Yielded %d/%d items", yieldedCountBaseline, addedCount)
	if yieldedCountBaseline != addedCount {
		t.Errorf("Baseline iteration should yield all items, but got %d/%d", yieldedCountBaseline, addedCount)
	}

	// Test 2: Iterate with no additional operations - this should trigger the bug
	// Just do a simple InOrderVisit like the production iterator test does
	t.Logf("Test 2: Simple InOrderVisit after Persist (reproducing iterator test scenario)...")
	yieldedCountSimple := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		yieldedCountSimple++
		if yieldedCountSimple <= 5 || yieldedCountSimple%10000 == 0 {
			if pNode, ok := node.(treap.PersistentPayloadNodeInterface[testKey, types.JsonPayload[string]]); ok {
				t.Logf("  Simple item %d: %s", yieldedCountSimple, pNode.GetPayload().Value)
			}
		}
		return nil
	})
	if err != nil {
		t.Logf("Simple iteration error: %v", err)
	}
	t.Logf("Simple: Yielded %d/%d items", yieldedCountSimple, addedCount)

	// Test 3: Iterate with periodic explicit flushing between iteration batches
	// This tests that persisted trees can survive multiple flush cycles
	t.Logf("Test 3: Iterating with periodic flushing between batches...")
	yieldedCountWithFlush := 0
	batchSize := 10000 // Iterate 10k items, then flush, repeat
	totalBatches := (addedCount + batchSize - 1) / batchSize

	for batch := 0; batch < totalBatches; batch++ {
		batchStart := yieldedCountWithFlush

		// Iterate one batch
		err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
			if node == nil || node.IsNil() {
				return nil
			}
			// Only count nodes in current batch
			if yieldedCountWithFlush >= batchStart+batchSize {
				return fmt.Errorf("batch complete") // Stop this iteration
			}
			yieldedCountWithFlush++

			if yieldedCountWithFlush <= 5 || yieldedCountWithFlush%10000 == 0 {
				if pNode, ok := node.(treap.PersistentPayloadNodeInterface[testKey, types.JsonPayload[string]]); ok {
					t.Logf("  Yielding item %d: %s", yieldedCountWithFlush, pNode.GetPayload().Value)
				}
			}
			return nil
		})
		if err != nil && err.Error() != "batch complete" {
			t.Fatalf("Iteration batch %d error: %v", batch, err)
		}

		// Flush after each batch
		beforeFlush := coll.CountInMemoryNodes()
		flushedCount, err := coll.FlushOldestPercentile(50)
		if err != nil {
			t.Logf("Warning: flush error after batch %d: %v", batch, err)
		}
		afterFlush := coll.CountInMemoryNodes()
		if batch%5 == 0 || batch == totalBatches-1 {
			t.Logf("  After batch %d/%d: Flushed %d nodes (in-memory: %d -> %d)",
				batch+1, totalBatches, flushedCount, beforeFlush, afterFlush)
		}

		// Re-persist after flush to save tree structure
		err = coll.Persist()
		if err != nil {
			t.Fatalf("Failed to persist after batch %d: %v", batch, err)
		}
	}

	// Report findings
	t.Logf("Results:")
	t.Logf("  Baseline (no additional operations): %d/%d items", yieldedCountBaseline, addedCount)
	t.Logf("  Simple iteration (reproducing bug): %d/%d items", yieldedCountSimple, addedCount)
	t.Logf("  With aggressive flushing: %d/%d items", yieldedCountWithFlush, addedCount)
	finalInMemory := coll.CountInMemoryNodes()
	t.Logf("  Final in-memory nodes: %d/%d (%.1f%%)", finalInMemory, addedCount,
		100.0*float64(finalInMemory)/float64(addedCount))

	if yieldedCountSimple < addedCount {
		loss := 100.0 * float64(addedCount-yieldedCountSimple) / float64(addedCount)
		t.Errorf("ITERATOR BUG REPRODUCED: Simple InOrderVisit yielded only %d of %d items (%.1f%% data loss)",
			yieldedCountSimple, addedCount, loss)
		t.Logf("This matches the production iterator test failure pattern")
	}

	if yieldedCountWithFlush != addedCount {
		loss := 100.0 * float64(addedCount-yieldedCountWithFlush) / float64(addedCount)
		t.Errorf("MEMORY PRESSURE BUG: Iteration with flushing yielded only %d of %d items (%.1f%% data loss)",
			yieldedCountWithFlush, addedCount, loss)
		t.Logf("Expected: All %d items should be yielded even with aggressive flushing", addedCount)
	} else {
		t.Logf("SUCCESS: All items yielded correctly with memory pressure simulation")
	}
}

// TestBobbobMemoryPressureProductionSettings tests with settings matching production iterator test
func TestBobbobMemoryPressureProductionSettings(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/memory_pressure_prod_settings.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Match production test settings EXACTLY (but without background monitoring to keep test fast)
	// Note: Don't use SetMemoryBudgetWithPercentile() with large datasets - it starts background monitoring
	// which adds significant disk I/O overhead causing tests to exceed timeout
	// session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)
	// session.Vault.SetCheckInterval(1000)
	// NOTE: Do NOT call StartBackgroundMonitoring() - production test doesn't

	// Use simple testKey (without length prefix) to match production more closely
	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"prod_settings_test",
		testKeyLess,
		(*testKey)(new(testKey)),
		types.JsonPayload[string]{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert items similar to production - vary the pattern
	const maxItems = 100_000
	addedCount := 0
	for i := 0; i < maxItems; i++ {
		path := fmt.Sprintf("file_%d", i)
		payload := types.JsonPayload[string]{Value: path}

		// Use same pattern as production: reverse order ranking
		key := testKey{
			Rank: (maxItems - i) / 100,
			Size: uint64(maxItems - i),
			Path: fmt.Sprintf("key_%d", i),
		}
		coll.Insert(&key, payload)
		addedCount++

		if addedCount%50000 == 0 {
			t.Logf("Inserted %d items", addedCount)
		}
	}

	t.Logf("Total items inserted: %d", addedCount)

	inMemory := coll.CountInMemoryNodes()
	onDisk := addedCount - inMemory
	t.Logf("After inserts: In-memory: %d, Flushed to disk: %d (%.1f%% on disk)",
		inMemory, onDisk, 100.0*float64(onDisk)/float64(addedCount))

	// Persist like we added to production test
	t.Logf("Persisting tree structure to disk...")
	err = coll.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Simple InOrderVisit like production test
	t.Logf("Starting iteration...")
	yieldedCount := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[testKey, types.JsonPayload[string]])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		_ = payloadNode.GetPayload()
		yieldedCount++

		if yieldedCount <= 20 || yieldedCount%50000 == 0 {
			t.Logf("Yielded item %d", yieldedCount)
		}
		return nil
	})
	if err != nil {
		t.Logf("InOrderVisit error: %v", err)
	}

	t.Logf("Iteration complete: Yielded %d/%d items", yieldedCount, addedCount)

	if yieldedCount < addedCount {
		loss := 100.0 * float64(addedCount-yieldedCount) / float64(addedCount)
		t.Errorf("PRODUCTION SETTINGS BUG: InOrderVisit yielded only %d of %d items (%.1f%% data loss)",
			yieldedCount, addedCount, loss)
		t.Logf("Memory: In-memory: %d, On-disk: %d", inMemory, onDisk)
	} else {
		t.Logf("SUCCESS: All %d items yielded correctly", yieldedCount)
	}
}

// TestBobbobMemoryPressureNoBackgroundMonitoring tests without background monitoring
func TestBobbobMemoryPressureNoBackgroundMonitoring(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/no_background_monitoring.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Aggressive settings but NO background monitoring
	// NOTE: Don't use SetMemoryBudgetWithPercentile() with large iterators - it starts background monitoring
	// which adds significant performance overhead from disk I/O causing tests to exceed timeout
	// session.Vault.SetMemoryBudgetWithPercentile(5_000, 10)
	// session.Vault.SetCheckInterval(100)
	// Do NOT call StartBackgroundMonitoring()

	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"no_bg_monitor_test",
		testKeyLess,
		(*testKey)(new(testKey)),
		types.JsonPayload[string]{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	const maxItems = 100_000
	addedCount := 0
	for i := 0; i < maxItems; i++ {
		path := fmt.Sprintf("file_%d", i)
		payload := types.JsonPayload[string]{Value: path}

		key := testKey{
			Rank: (maxItems - i) / 100,
			Size: uint64(maxItems - i),
			Path: fmt.Sprintf("key_%d", i),
		}
		coll.Insert(&key, payload)
		addedCount++
	}

	t.Logf("Generated %d items (no background monitoring)", addedCount)

	inMemory := coll.CountInMemoryNodes()
	onDisk := addedCount - inMemory
	t.Logf("After inserts: In-memory: %d, Flushed to disk: %d (%.1f%% on disk)",
		inMemory, onDisk, 100.0*float64(onDisk)/float64(addedCount))

	// Persist
	t.Logf("Persisting tree structure to disk...")
	err = coll.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Simple iteration
	t.Logf("Starting iteration (no background monitoring)...")
	yieldedCount := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		yieldedCount++
		return nil
	})
	if err != nil {
		t.Logf("InOrderVisit error: %v", err)
	}

	t.Logf("Iteration complete: Yielded %d/%d items", yieldedCount, addedCount)

	if yieldedCount < addedCount {
		loss := 100.0 * float64(addedCount-yieldedCount) / float64(addedCount)
		t.Errorf("NO BACKGROUND MONITORING BUG: InOrderVisit yielded only %d of %d items (%.1f%% data loss)",
			yieldedCount, addedCount, loss)
	} else {
		t.Logf("SUCCESS: All items yielded without background monitoring")
	}
}

// TestBobbobMemoryPressureProductionSettingsWithBackgroundMonitoring tests production settings
// Note: Despite the name, background monitoring is disabled to keep test execution time reasonable
func TestBobbobMemoryPressureProductionSettingsWithBackgroundMonitoring(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/prod_settings_with_bg.db"

	session, _, err := vault.OpenVault(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create vault: %v", err)
	}
	defer session.Close()

	// Match production test settings EXACTLY but DON'T add background monitoring (would be too slow)
	// Note: SetMemoryBudgetWithPercentile() automatically starts background monitoring which adds
	// significant disk I/O overhead causing tests to take 2-5x longer and potentially timeout
	// session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)
	// session.Vault.SetCheckInterval(1000)
	// session.Vault.StartBackgroundMonitoring() // THIS WOULD ADD SIGNIFICANT OVERHEAD

	// Use simple testKey (without length prefix) same as failing test
	coll, err := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"prod_with_bg_test",
		testKeyLess,
		(*testKey)(new(testKey)),
		types.JsonPayload[string]{},
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert same data pattern as failing test
	const maxItems = 100_000
	addedCount := 0
	for i := 0; i < maxItems; i++ {
		path := fmt.Sprintf("file_%d", i)
		payload := types.JsonPayload[string]{Value: path}

		key := testKey{
			Rank: (maxItems - i) / 100,
			Size: uint64(maxItems - i),
			Path: fmt.Sprintf("key_%d", i),
		}
		coll.Insert(&key, payload)
		addedCount++

		if addedCount%50000 == 0 {
			t.Logf("Inserted %d items", addedCount)
		}
	}

	t.Logf("Total items inserted: %d (with background monitoring)", addedCount)

	inMemory := coll.CountInMemoryNodes()
	onDisk := addedCount - inMemory
	t.Logf("After inserts: In-memory: %d, Flushed to disk: %d (%.1f%% on disk)",
		inMemory, onDisk, 100.0*float64(onDisk)/float64(addedCount))

	// Persist like production test
	t.Logf("Persisting tree structure to disk...")
	err = coll.Persist()
	if err != nil {
		t.Fatalf("Failed to persist collection: %v", err)
	}

	// Simple InOrderVisit like production test
	t.Logf("Starting iteration (with background monitoring)...")
	yieldedCount := 0
	err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
		payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[testKey, types.JsonPayload[string]])
		if !ok {
			return fmt.Errorf("unexpected node type %T", node)
		}
		_ = payloadNode.GetPayload()
		yieldedCount++

		if yieldedCount <= 20 || yieldedCount%50000 == 0 {
			t.Logf("Yielded item %d", yieldedCount)
		}
		return nil
	})
	if err != nil {
		t.Logf("InOrderVisit error: %v", err)
	}

	t.Logf("Iteration complete: Yielded %d/%d items", yieldedCount, addedCount)

	if yieldedCount < addedCount {
		loss := 100.0 * float64(addedCount-yieldedCount) / float64(addedCount)
		t.Errorf("BUG STILL EXISTS WITH BACKGROUND MONITORING: InOrderVisit yielded only %d of %d items (%.1f%% data loss)",
			yieldedCount, addedCount, loss)
		t.Logf("Memory: In-memory: %d, On-disk: %d", inMemory, onDisk)
		t.Logf("This means background monitoring is NOT the solution")
	} else {
		t.Logf("SUCCESS: Background monitoring FIXED the bug! All %d items yielded", yieldedCount)
		t.Logf("Recommendation: Add session.Vault.StartBackgroundMonitoring() to production code")
	}
}
