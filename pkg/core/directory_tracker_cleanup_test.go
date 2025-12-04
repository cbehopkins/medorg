package core

import (
	"sync/atomic"
	"testing"
)

// TestDirectoryTrackerResourceCleanup ensures goroutines exit and channels close on error
func TestDirectoryTrackerResourceCleanup(t *testing.T) {
	dir := t.TempDir()
	var closed int32
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mdt := newMockDtType()
		// Wrap in a tracking closure that sets a flag before closing
		tracker := &trackedMockDtType{
			mockDtType: mdt,
			onClose: func() {
				atomic.StoreInt32(&closed, 1)
			},
		}
		return tracker, nil
	}
	errChan := NewDirTracker(false, dir, makerFunc).ErrChan()
	for range errChan {
		// drain
	}
	if atomic.LoadInt32(&closed) == 0 {
		t.Error("expected Close to be called on DirectoryTrackerInterface")
	}
}

// trackedMockDtType wraps mockDtType to track Close calls
type trackedMockDtType struct {
	mockDtType
	onClose func()
}

func (t *trackedMockDtType) Close() {
	t.onClose()
	t.mockDtType.Close()
}
