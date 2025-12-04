package core

import (
	"io/fs"
	"strings"
	"testing"
)

type notDirectoryEntry struct{}

func (notDirectoryEntry) ErrChan() <-chan error                                      { return nil }
func (notDirectoryEntry) Start() error                                               { return nil }
func (notDirectoryEntry) Close()                                                     {}
func (notDirectoryEntry) VisitFile(dir, file string, d fs.DirEntry, callback func()) {}

func TestRevisitAllTypeAssertionPanic(t *testing.T) {
	dt := &DirTracker{
		dm: map[string]DirectoryTrackerInterface{
			"bad": notDirectoryEntry{},
		},
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic, got none")
			return
		}
		if msg, ok := r.(string); ok {
			if want := "RevisitAll: entry for path bad is not of type DirectoryEntry"; !strings.Contains(msg, want) {
				t.Errorf("panic message does not contain expected text: got %q", msg)
			}
		} else {
			t.Errorf("expected panic to be a string, got %T: %v", r, r)
		}
	}()
	dt.RevisitAll("", nil, nil, nil)
}
