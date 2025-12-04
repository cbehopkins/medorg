package core

import (
	"testing"
)

// TestDirTrackerImplementsProgressable verifies that DirTracker implements
// the pb.Progressable interface by checking all required methods exist.
// This test would have caught the missing FinishedChan() method.
func TestDirTrackerImplementsProgressable(t *testing.T) {
	dir := t.TempDir()

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		return newMockDtType(), nil
	}

	dt := NewDirTracker(false, dir, makerFunc)

	// Verify all Progressable interface methods exist and have correct signatures

	// 1. Total() int64
	total := dt.Total()
	if total < 0 {
		t.Errorf("Total() returned negative value: %d", total)
	}

	// 2. Value() int64
	value := dt.Value()
	if value < 0 {
		t.Errorf("Value() returned negative value: %d", value)
	}

	// 3. FinishedChan() <-chan struct{}
	finishedChan := dt.FinishedChan()
	if finishedChan == nil {
		t.Error("FinishedChan() returned nil channel")
	}

	// Wait for completion and verify the channel gets closed
	errChan := dt.ErrChan()
	for err := range errChan {
		if err != nil {
			t.Logf("Got error during traversal: %v", err)
		}
	}

	// After errChan closes, finishedChan should also be closed
	select {
	case <-finishedChan:
		// Good - channel was closed as expected
	default:
		t.Error("FinishedChan() was not closed after directory tracking completed")
	}

	// Verify Finished() boolean matches channel state
	if !dt.Finished() {
		t.Error("Finished() returned false after completion")
	}
}

// TestDirTrackerFinishedChanClosesOnCompletion ensures the finished channel
// closes when directory tracking completes, not before.
func TestDirTrackerFinishedChanClosesOnCompletion(t *testing.T) {
	dir := t.TempDir()

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		return newMockDtType(), nil
	}

	dt := NewDirTracker(false, dir, makerFunc)
	finishedChan := dt.FinishedChan()

	// Channel should NOT be closed immediately
	select {
	case <-finishedChan:
		t.Error("FinishedChan() closed prematurely before traversal completed")
	default:
		// Expected - channel still open
	}

	// Drain error channel to completion
	errChan := dt.ErrChan()
	for range errChan {
		// consume all errors
	}

	// Now channel SHOULD be closed
	select {
	case <-finishedChan:
		// Expected - channel closed after completion
	default:
		t.Error("FinishedChan() was not closed after traversal completed")
	}
}

// TestDirTrackerProgressableInterfaceCompiletime is a compile-time check
// that DirTracker satisfies a Progressable-like interface.
func TestDirTrackerProgressableInterfaceCompiletime(t *testing.T) {
	// This interface mimics pb.Progressable
	type Progressable interface {
		Total() int64
		Value() int64
		FinishedChan() <-chan struct{}
	}

	// This will fail to compile if DirTracker doesn't implement all methods
	var _ Progressable = (*DirTracker)(nil)

	t.Log("DirTracker correctly implements Progressable interface")
}
