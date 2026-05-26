package core

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// helper to create files and persist a directory map for a directory
func writeDirMap(t *testing.T, dir string, files []string) int {
	t.Helper()
	dm := newDirectoryMap()
	count := 0
	for _, name := range files {
		// ensure directory exists
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		fp := filepath.Join(dir, name)
		if err := os.WriteFile(fp, []byte("data"), 0o644); err != nil {
			t.Fatalf("write file %s: %v", fp, err)
		}
		fs, err := NewFileStruct(dir, name)
		if err != nil {
			t.Fatalf("NewFileStruct for %s: %v", fp, err)
		}
		dm.Add(fs)
		count++
	}
	if err := dm.Persist(Dirname(dir)); err != nil {
		t.Fatalf("persist dm for %s: %v", dir, err)
	}
	return count
}

type mockDtType struct {
	errChan chan error
	lock    *sync.RWMutex
	closed  *bool
	visiter func(Dirname, Fname)
}

func newMockDtType() (mdt mockDtType) {
	mdt.errChan = make(chan error)
	mdt.lock = new(sync.RWMutex)
	mdt.closed = new(bool)
	return
}

func (mdt mockDtType) ErrChan() <-chan error {
	return mdt.errChan
}

func (mdt mockDtType) Start() error {
	return nil
}

func (mdt mockDtType) Close() {
	mdt.lock.Lock()
	*mdt.closed = true
	mdt.lock.Unlock()
	close(mdt.errChan)
}

var errTestChanClosed = errors.New("visit called to a closed structure")

func (mdt mockDtType) Visitor(dir Dirname, file Fname, d fs.DirEntry) error {
	mdt.lock.RLock()
	closed := *mdt.closed
	mdt.lock.RUnlock()
	if closed {
		return fmt.Errorf("%w at %s/%s", errTestChanClosed, string(dir), string(file))
	}
	if mdt.visiter != nil {
		mdt.visiter(dir, file)
	}
	return nil
}

func (mdt mockDtType) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
	mdt.lock.Lock()
	if *mdt.closed {
		mdt.errChan <- fmt.Errorf("%w at %s/%s", errTestChanClosed, dir, file)
	}
	mdt.lock.Unlock()

	if mdt.visiter != nil {
		mdt.visiter(Dirname(dir), Fname(file))
	}
	callback()
}
