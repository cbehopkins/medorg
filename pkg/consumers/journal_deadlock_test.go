package consumers

import (
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestJournal_ZeroValueDeadlock demonstrates that zero-value initialization
// causes a deadlock because entryChan is nil
func TestJournal_ZeroValueDeadlock(t *testing.T) {
	// This mimics what the actual application does
	journal := Journal{} // Zero-value initialization - NO NewJournal() call!

	dm := core.NewDirectoryMap()
	fs := core.FileStruct{}
	fs.SetDirectory("/test")
	fs.Name = "test.txt"
	fs.Size = 100
	fs.Checksum = "abc123"
	dm.Add(fs)

	// This should deadlock because entryChan is nil
	done := make(chan bool)
	go func() {
		// This will block forever on: jo.entryChan <- entry
		err := journal.AppendJournalFromDm(dm, "/test/dir")
		if err != nil {
			t.Logf("AppendJournalFromDm error: %v", err)
		}
		done <- true
	}()

	select {
	case <-done:
		t.Log("SUCCESS: AppendJournalFromDm completed with zero-value initialization (synchronous mode)")
	case <-time.After(1 * time.Second):
		t.Error("AppendJournalFromDm timed out - this should not happen after the fix")
	}
}

// TestJournal_ProperInitializationWorks shows that NewJournal() works correctly
func TestJournal_ProperInitializationWorks(t *testing.T) {
	// Proper initialization
	journal := NewJournal(10)
	defer journal.Flush()

	dm := core.NewDirectoryMap()
	fs := core.FileStruct{}
	fs.SetDirectory("/test")
	fs.Name = "test.txt"
	fs.Size = 100
	fs.Checksum = "abc123"
	dm.Add(fs)

	// This should work fine
	done := make(chan bool)
	go func() {
		err := journal.AppendJournalFromDm(dm, "/test/dir")
		if err != nil && err != ErrFileExistsInJournal {
			t.Errorf("AppendJournalFromDm error: %v", err)
		}
		done <- true
	}()

	select {
	case <-done:
		t.Log("SUCCESS: AppendJournalFromDm completed with proper initialization")
	case <-time.After(1 * time.Second):
		t.Error("AppendJournalFromDm timed out even with proper initialization")
	}
}

// TestJournal_CheckNilChannels verifies the zero-value has nil channels
func TestJournal_CheckNilChannels(t *testing.T) {
	journal := Journal{}

	if journal.entryChan != nil {
		t.Error("Expected entryChan to be nil in zero-value Journal")
	}
	if journal.stopChan != nil {
		t.Error("Expected stopChan to be nil in zero-value Journal")
	}
	if journal.location != nil {
		t.Error("Expected location to be nil in zero-value Journal")
	}

	t.Log("Confirmed: Zero-value Journal has nil channels - this causes deadlocks!")
}
