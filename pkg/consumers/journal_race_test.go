package consumers

import (
	"sync"
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestJournal_ConcurrentAppendAndFlush demonstrates the race condition
// where appendItem can panic when Flush is called concurrently
func TestJournal_ConcurrentAppendAndFlush(t *testing.T) {
	// This test demonstrates the bug where:
	// 1. appendItem sends to entryChan (line 148)
	// 2. Flush closes stopChan, which causes entryWriter to close entryChan
	// 3. appendItem panics with "send on closed channel"

	// Run this test multiple times to increase chance of hitting the race
	for iteration := 0; iteration < 10; iteration++ {
		t.Run("iteration", func(t *testing.T) {
			journal := NewJournal(10)

			// Create a dummy DirectoryMap
			dm := core.NewDirectoryMap()
			fs := core.FileStruct{}
			fs.SetDirectory("/test")
			fs.Name = "test.txt"
			fs.Size = 100
			fs.Checksum = "abc123"
			dm.Add(fs)

			var wg sync.WaitGroup
			errors := make(chan interface{}, 100)

			// Goroutine 1: Keep appending items
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						// Catch the panic that occurs when sending on closed channel
						errors <- r
						t.Logf("CAUGHT PANIC (this demonstrates the bug): %v", r)
					}
				}()

				for i := 0; i < 100; i++ {
					err := journal.appendItem(dm, "/test/dir", "alias")
					if err != nil {
						t.Logf("appendItem error: %v", err)
					}
					// Small delay to increase interleaving probability
					time.Sleep(time.Microsecond)
				}
			}()

			// Goroutine 2: Flush the journal mid-way
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Wait a bit to let some appends happen
				time.Sleep(50 * time.Microsecond)
				journal.Flush()
			}()

			wg.Wait()
			close(errors)

			// Check if we caught a panic
			panicCount := 0
			for panicVal := range errors {
				panicCount++
				t.Logf("Panic detected: %v", panicVal)
			}

			if panicCount > 0 {
				t.Errorf("Race condition detected: %d panic(s) occurred when Flush was called while appendItem was sending", panicCount)
			}
		})
	}
}

// TestJournal_DoubleFlush demonstrates another potential issue
func TestJournal_DoubleFlush(t *testing.T) {
	journal := NewJournal(10)

	dm := core.NewDirectoryMap()
	fs := core.FileStruct{}
	fs.SetDirectory("/test")
	fs.Name = "test.txt"
	fs.Size = 100
	fs.Checksum = "abc123"
	dm.Add(fs)

	// Add some items
	_ = journal.appendItem(dm, "/test/dir1", "alias1")
	_ = journal.appendItem(dm, "/test/dir2", "alias2")

	// First flush
	journal.Flush()

	// Second flush - this could cause issues
	// Note: Currently this will hang or panic because stopChan is already closed
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Double flush caused panic: %v", r)
		}
	}()

	// Attempting to flush again
	done := make(chan bool)
	go func() {
		journal.Flush() // This will likely panic on "close of closed channel"
		done <- true
	}()

	select {
	case <-done:
		t.Log("Second flush completed (unexpectedly)")
	case <-time.After(100 * time.Millisecond):
		t.Log("Second flush timed out or deadlocked (expected)")
	}
}

// TestJournal_AppendAfterFlush demonstrates sending after flush
func TestJournal_AppendAfterFlush(t *testing.T) {
	journal := NewJournal(10)

	dm := core.NewDirectoryMap()
	fs := core.FileStruct{}
	fs.SetDirectory("/test")
	fs.Name = "test.txt"
	fs.Size = 100
	fs.Checksum = "abc123"
	dm.Add(fs)

	// Add an item
	_ = journal.appendItem(dm, "/test/dir1", "alias1")

	// Flush
	journal.Flush()

	// Try to append after flush - this should now return an error instead of panicking
	err := journal.appendItem(dm, "/test/dir2", "alias2")
	if err == nil {
		t.Error("Expected error when appending after flush, but got nil")
	} else {
		t.Logf("Got expected error when appending after flush: %v", err)
	}
} // TestJournal_ConcurrentFlushes demonstrates multiple concurrent flushes
func TestJournal_ConcurrentFlushes(t *testing.T) {
	journal := NewJournal(10)

	dm := core.NewDirectoryMap()
	fs := core.FileStruct{}
	fs.SetDirectory("/test")
	fs.Name = "test.txt"
	fs.Size = 100
	fs.Checksum = "abc123"
	dm.Add(fs)

	// Add some items
	for i := 0; i < 5; i++ {
		_ = journal.appendItem(dm, "/test/dir", "alias")
	}

	var wg sync.WaitGroup
	panics := make(chan interface{}, 10)

	// Try multiple concurrent flushes
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics <- r
					t.Logf("Flush %d panicked: %v", id, r)
				}
			}()
			journal.Flush()
		}(i)
	}

	wg.Wait()
	close(panics)

	panicCount := 0
	for range panics {
		panicCount++
	}

	if panicCount > 0 {
		t.Errorf("Concurrent flushes caused %d panic(s)", panicCount)
	}
}
