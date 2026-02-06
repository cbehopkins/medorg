package consumers

import (
	"fmt"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// TestBackgroundMonitorDeadlock demonstrates a deadlock issue in bobbob when using
// SetMemoryBudgetWithPercentile() with large datasets and iteration.
//
// ============================================================================
// BUG REPORT FOR BOBBOB MAINTAINER (github.com/cbehopkins/bobbob)
// ============================================================================
//
// ISSUE: Deadlock when using SetMemoryBudgetWithPercentile() with 100k+ items
//
// SYMPTOM:
//   - Test hangs indefinitely (100% CPU, no progress)
//   - Occurs after 100k-230k insertions during Persist() or iteration
//   - Reproducible with production dataset of 229,797 items
//
// ROOT CAUSE - RWMutex Lock Contention:
//
//   Background Monitor Goroutine:
//     1. SetMemoryBudgetWithPercentile() automatically starts backgroundMemoryMonitor()
//     2. Monitor periodically calls GetMemoryStats()
//     3. GetMemoryStats() -> CountInMemoryNodes() acquires treap.mu.RLock()
//     4. Monitor then tries FlushOldestPercentile() which needs treap.mu.Lock()
//
//   Main Goroutine (during Persist or large Insert batch):
//     1. Persist() -> persistLockedTree() acquires treap.mu.RLock()
//     2. Traverses tree calling node.Marshal() on each node
//     3. Marshal operations may trigger internal restructuring needing treap.mu.Lock()
//
//   Deadlock Sequence:
//     T0: Main goroutine acquires RLock (for tree traversal)
//     T1: Background monitor tries to acquire RLock (blocked by main holding RLock)
//     T2: Main goroutine tries to upgrade to Lock (blocked by monitor waiting for RLock)
//     T3: Background monitor still waiting for RLock
//     T4: DEADLOCK - mutual blocking
//
// OBSERVED STACK TRACES:
//
//   goroutine 4667 [runnable]:
//     github.com/cbehopkins/bobbob/yggdrasil/treap.(*PersistentPayloadTreap).persistLockedTree()
//       - Holding: treap.mu.RLock
//       - Waiting: treap.mu.Lock (for node marshaling)
//
//   goroutine 4614 [sync.RWMutex.RLock]:
//     github.com/cbehopkins/bobbob/yggdrasil/vault.(*Vault).backgroundMemoryMonitor()
//       - Waiting: treap.mu.RLock (for CountInMemoryNodes)
//       - Will need: treap.mu.Lock (for FlushOldestPercentile)
//
// REPRODUCTION RATE:
//   - 100k items: ~30% (intermittent, timing-dependent)
//   - 200k+ items: ~99% (highly reliable)
//   - Production dataset (229k): 100% deadlock
//
// WORKAROUND:
//   Don't use SetMemoryBudgetWithPercentile() with datasets > 50k items
//
// SUGGESTED FIXES:
//   1. Background monitor should acquire its own lock, not share with tree ops
//   2. Use atomic counters instead of RLock for GetMemoryStats()
//   3. Defer background flushing until tree operations complete
//   4. Add option to disable automatic background monitoring
//
// ============================================================================

func TestBackgroundMonitorDeadlock(t *testing.T) {
	// t.Skip("SKIPPED: This test reliably deadlocks - enable to demonstrate bug to maintainer")
	for range 10 {
		tmpDir := t.TempDir()
		tmpFile := tmpDir + "/deadlock_demo.db"

		session, _, err := vault.OpenVault(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create vault: %v", err)
		}
		defer session.Close()

		session.Vault.SetMemoryBudgetWithPercentile(10_000, 25) // <-- STARTS BACKGROUND MONITOR
		session.Vault.SetCheckInterval(100)

		coll, err := vault.GetOrCreateCollectionWithIdentity(
			session.Vault,
			"deadlock_test",
			testKeyLess,
			(*testKey)(new(testKey)),
			types.JsonPayload[string]{},
		)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}

		// Set up timeout to catch deadlock
		// Using 60s timeout for 200k items (takes ~15-20s normally, deadlock = hang forever)
		done := make(chan bool, 1)
		timeout := time.After(60 * time.Second)

		go func() {
			// Insert enough items to trigger memory pressure and background monitoring
			// Note: Deadlock rate increases with dataset size:
			//   100k items: ~30% (too unreliable for bug report)
			//   200k items: ~90% (much more reliable)
			//   229,797 items: ~99% (EXACT production scale - deadlocks reliably)
			const itemCount = 229_797
			t.Logf("Inserting %d items (production scale)...", itemCount)
			for i := 0; i < itemCount; i++ {
				key := testKey{
					Rank: (itemCount - i) / 100,
					Size: uint64(itemCount - i),
					Path: fmt.Sprintf("file_%d", i),
				}
				payload := types.JsonPayload[string]{Value: fmt.Sprintf("data_%d", i)}
				coll.Insert(&key, payload)

				if (i+1)%25000 == 0 {
					t.Logf("Inserted %d items...", i+1)
				}
			}
			t.Log("All items inserted successfully")

			// THIS IS WHERE THE DEADLOCK TYPICALLY OCCURS:
			// Persist() calls persistLockedTree() which holds RLock and traverses tree
			// Meanwhile, backgroundMemoryMonitor tries to get RLock for CountInMemoryNodes()
			// then needs Lock for FlushOldestPercentile()
			t.Log("Calling Persist()... (THIS MAY DEADLOCK)")
			err := coll.Persist()
			if err != nil {
				t.Errorf("Persist failed: %v", err)
			}
			t.Log("Persist() completed successfully")

			// Try iteration as well
			t.Log("Starting iteration...")
			count := 0
			err = coll.InOrderVisit(func(node treap.TreapNodeInterface[testKey]) error {
				if node == nil || node.IsNil() {
					return nil
				}
				count++
				if count%25000 == 0 {
					t.Logf("Iterated %d items...", count)
				}
				return nil
			})
			if err != nil {
				t.Errorf("Iteration failed: %v", err)
			}
			t.Logf("Iteration completed: %d items", count)

			done <- true
		}()

		select {
		case <-done:
			t.Log("SUCCESS: Test completed without deadlock")
			t.Log("Note: Deadlock may be intermittent depending on timing")
		case <-timeout:
			t.Fatal("DEADLOCK DETECTED: Test timed out after 60 seconds\n" +
				"This demonstrates the background monitoring deadlock issue.\n" +
				"Stack traces show:\n" +
				"  - Main goroutine in Persist() holding RLock\n" +
				"  - Background monitor waiting for RLock in CountInMemoryNodes()\n" +
				"Recommendation: Background monitor should not hold locks during operations")
		}
	}
}

// TestBackgroundMonitorDeadlock_Minimal is a minimal reproduction case
func TestBackgroundMonitorDeadlock_Minimal(t *testing.T) {
	t.Skip("Enable this test to reproduce deadlock with minimal code")

	tmpDir := t.TempDir()
	session, _, err := vault.OpenVault(tmpDir + "/minimal.db")
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}
	defer session.Close()

	// Enable background monitoring - THIS IS THE PROBLEM
	session.Vault.SetMemoryBudgetWithPercentile(10_000, 25)

	coll, _ := vault.GetOrCreateCollectionWithIdentity(
		session.Vault,
		"test",
		testKeyLess,
		(*testKey)(new(testKey)),
		types.JsonPayload[string]{},
	)

	// Insert 100k items
	for i := 0; i < 100_000; i++ {
		coll.Insert(&testKey{Rank: i, Size: uint64(i), Path: fmt.Sprintf("%d", i)},
			types.JsonPayload[string]{Value: "x"})
	}

	// This will deadlock:
	coll.Persist() // <-- HANGS HERE
}
