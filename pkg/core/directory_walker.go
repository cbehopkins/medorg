package core

import (
	"io/fs"
	"path/filepath"
	"sync/atomic"
)

type directoryWalker struct {
	shouldIgnore func(path string) bool // Optional function to check if path should be ignored
	WorkTokens   chan struct{}
	DirectoryVisitor func(path string, d fs.DirEntry, err error) error
	cancelChan   chan struct{}
}
type DirectoryWalker struct {
	directoryWalker
	FileVisitor  func(string, FileMetadata) error

}
func (dw directoryWalker) grabWorkToken() {
	if dw.WorkTokens != nil {
		<-dw.WorkTokens
	}
}
func (dw directoryWalker) releaseWorkToken() {
	if dw.WorkTokens != nil {
		dw.WorkTokens <- struct{}{}
	}
}
func NewDirectoryWalker(WorkTokens chan struct{}) *DirectoryWalker {
	dw:= &DirectoryWalker{
		directoryWalker: directoryWalker{
			WorkTokens: WorkTokens,
			cancelChan: make(chan struct{}),
		},
	}
	dw.directoryWalker.DirectoryVisitor = dw.dirVisitor
	return dw
}
func (dw *directoryWalker) Cancel() {
	close(dw.cancelChan)
}
func (dw *directoryWalker) Walk(root string) error {
	return filepath.WalkDir(root, dw.walkVisitor)
}

func (dw *directoryWalker) shouldSkipDir(path string, d fs.DirEntry) error {

	if dw.cancelChan != nil {
		select {
		case <-dw.cancelChan:
			return filepath.SkipAll
		default:
		}
	}

	// Skip hidden directories
	if isHiddenDirectory(path) {
		return filepath.SkipDir
	}
	if hasSkipfile(path) {
		return filepath.SkipDir
	}
	// Skip directories matching ignore patterns
	if dw.shouldIgnore != nil && dw.shouldIgnore(path) {
		return filepath.SkipDir
	}
	return nil
}
func (dw *directoryWalker) walkVisitor(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if !d.IsDir() {
		return nil
	}
	err = dw.shouldSkipDir(path, d)
	if err != nil {
		return err
	}
	if d.IsDir() {
		if dw.DirectoryVisitor != nil {
			return dw.DirectoryVisitor(path, d, err)
		}
		return nil
	} 
	return nil
	
}
func (dw *DirectoryWalker) dirVisitor(path string, d fs.DirEntry, err error) error {
	// FIXME could we throw this into a worker pool?
	dw.grabWorkToken()
	dm, err := DirectoryMapFromDirWithScan(path)
	dm.UpdateAllChecksums()
	dw.releaseWorkToken()
	if err != nil {
		return err
	}
	if dw.FileVisitor == nil {
		return nil
	}

	return dm.ForEachFile(dw.FileVisitor)
}
type ProgressableDirectoryWalker struct {
	DirectoryWalker
	dirCount directoryWalker
	Progress *directoryWalkerProgress
}

func NewProgressableDirectoryWalker(WorkTokens chan struct{}, path string) *ProgressableDirectoryWalker {
	dw := NewDirectoryWalker(WorkTokens)
	pdw := &ProgressableDirectoryWalker{
		DirectoryWalker: *dw,
		Progress: &directoryWalkerProgress{finishedChan: make(chan struct{})},
	}
	
	// Re-bind the dirVisitor method to the copied DirectoryWalker
	pdw.DirectoryWalker.directoryWalker.DirectoryVisitor = pdw.DirectoryWalker.dirVisitor

	// Counting walker shares the same ignore/cancel logic but only increments totals.
	pdw.dirCount = directoryWalker{
		WorkTokens:   nil, // counting pass is lightweight; no token gate needed
		// This needs to come from the Ctrl-C handler to be effective
		cancelChan:   pdw.DirectoryWalker.directoryWalker.cancelChan,
		shouldIgnore: pdw.DirectoryWalker.directoryWalker.shouldIgnore,
	}
	pdw.dirCount.DirectoryVisitor = func(path string, d fs.DirEntry, err error) error {
		pdw.Progress.total.Add(1)
		return nil
	}

	// Wrap the primary DirectoryVisitor to record progress as directories are processed.
	baseVisitor := pdw.DirectoryWalker.directoryWalker.DirectoryVisitor
	pdw.DirectoryWalker.directoryWalker.DirectoryVisitor = func(path string, d fs.DirEntry, err error) error {
		pdw.Progress.value.Add(1)
		return baseVisitor(path, d, err)
	}

	return pdw
}

type directoryWalkerProgress struct {
	total        atomic.Int64
	value        atomic.Int64
	finishedChan chan struct{}
}
func NewDirectoryWalkerProgress(path string) *directoryWalkerProgress {
	
	dw:= &directoryWalkerProgress{
		finishedChan: make(chan struct{}),
	}
	return dw
}
func (dw *directoryWalkerProgress) Done() {
	close(dw.finishedChan)
}

// Progress interface methods
func (dw *directoryWalkerProgress) Total() int64{
	return dw.total.Load()
}
func (dw *directoryWalkerProgress) Value() int64 {
	return dw.value.Load()
}
func (dw *directoryWalkerProgress) FinishedChan() <-chan struct{}{
	return dw.finishedChan
}

// Walk performs a counting pass to establish progress totals, then runs the
// underlying DirectoryWalker while updating progress values.
// Calls Done() on the progress tracker when complete.
func (pdw *ProgressableDirectoryWalker) Walk(root string) error {
	// Reset progress
	pdw.Progress.total.Store(0)
	pdw.Progress.value.Store(0)

	// Keep ignore/cancel behaviour in sync with the primary walker
	pdw.dirCount.shouldIgnore = pdw.DirectoryWalker.directoryWalker.shouldIgnore
	pdw.dirCount.cancelChan = pdw.DirectoryWalker.directoryWalker.cancelChan

	// First pass: count directories
	if err := pdw.dirCount.Walk(root); err != nil {
		pdw.Progress.Done()
		return err
	}

	// Second pass: real traversal with progress increments
	err := pdw.DirectoryWalker.Walk(root)
	pdw.Progress.Done()
	return err
}
