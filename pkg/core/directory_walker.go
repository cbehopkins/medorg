package core

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
)

// This is a lighter weight version of DirectoryTracker
// It walks the directories without holding the full strcuture in memory
// This sacrifices IO for memory usage
// directoryWalker is the lower layer that handles directory traversal logic
// DirectoryWalker adds on file-level visitation
type DirectoryVisitor func(path Dirname, d fs.DirEntry, err error) error
type directoryWalker struct {
	shouldIgnore     func(path string) bool // Optional function to check if path should be ignored
	WorkTokens       chan struct{}
	DirectoryVisitor DirectoryVisitor
	cancelChan       chan struct{}
}
type DirectoryWalker struct {
	directoryWalker
	fileVisitors []ForEachCallback
}

func (dw *DirectoryWalker) AddFileVisitor(fv ForEachCallback) {
	dw.fileVisitors = append(dw.fileVisitors, fv)
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

// NewDirectoryWalker creates a DirectoryWalker
// You must supply a path to a work token chan.
// Typically you use the same one you already have
func NewDirectoryWalker(WorkTokens chan struct{}) *DirectoryWalker {
	dw := &DirectoryWalker{
		directoryWalker: directoryWalker{
			WorkTokens: WorkTokens,
			cancelChan: make(chan struct{}),
		},
	}
	dw.directoryWalker.DirectoryVisitor = dw.dirVisitor
	dw.fileVisitors = make([]ForEachCallback, 0)
	return dw
}
func (dw *directoryWalker) Cancel() {
	close(dw.cancelChan)
}
func (dw *directoryWalker) Walk(root string) error {
	// FIXME there is an optimisation we could do here
	// that collects up the file entries at the same time as walking
	// This would make doing the checkcalc much faster as we only need a single pass
	return filepath.WalkDir(root, dw.walkVisitor)
}
func(dw *directoryWalker) WalkMulti(roots []string) error {
	for _, root := range roots {
		if err := dw.Walk(root); err != nil {
			return err
		}
	}
	return nil
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

// Note the directoryWalker only visits directories
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
			return dw.DirectoryVisitor(Dirname(path), d, err)
		}
		return nil
	}
	return nil
}

// Note: this is DirectoryWalker and therefore we will now visit all files in the directory
func (dw *DirectoryWalker) dirVisitor(path Dirname, d fs.DirEntry, err error) error {
	// log.Printf("dirVisitor called for %s", path)
	// FIXME could we throw this into a worker pool?
	dw.grabWorkToken()
	defer dw.releaseWorkToken()

	dm, err := DirectoryMapFromDirWithScan(path)
	if err != nil {
		return err
	}

	// FIXME
	// See above comment about optimisation
	// We could collect up the file entries during the directory walk
	// and avoid this second pass
	if len(dw.fileVisitors) > 0 {
		err = dm.ForEachFile(func(fn Fname, fm FileMetadata, fi os.FileInfo) error {
			for _, fv := range dw.fileVisitors {
				if err := fv(fn, fm, fi); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return dm.Persist(path)
}

// ProgressableDirectoryWalker extends DirectoryWalker with progress tracking
// Which means we first need to count the number of directories to visit
// Then we can walk the directories updating progress as we go
type ProgressableDirectoryWalker struct {
	DirectoryWalker
	dirCount directoryWalker
	Progress *directoryWalkerProgress
}

func NewProgressableDirectoryWalker(WorkTokens chan struct{}, path string) *ProgressableDirectoryWalker {
	dw := NewDirectoryWalker(WorkTokens)
	pdw := &ProgressableDirectoryWalker{
		DirectoryWalker: *dw,
		Progress:        &directoryWalkerProgress{finishedChan: make(chan struct{})},
	}

	// Re-bind the dirVisitor method to the copied DirectoryWalker
	pdw.DirectoryWalker.directoryWalker.DirectoryVisitor = pdw.DirectoryWalker.dirVisitor

	// Counting walker shares the same ignore/cancel logic but only increments totals.
	pdw.dirCount = directoryWalker{
		WorkTokens: nil, // counting pass is lightweight; no token gate needed
		// This needs to come from the Ctrl-C handler to be effective
		cancelChan:   pdw.DirectoryWalker.directoryWalker.cancelChan,
		shouldIgnore: pdw.DirectoryWalker.directoryWalker.shouldIgnore,
	}
	pdw.dirCount.DirectoryVisitor = func(path Dirname, d fs.DirEntry, err error) error {
		pdw.Progress.total.Add(1)
		return nil
	}

	// Wrap the primary DirectoryVisitor to record progress as directories are processed.
	baseVisitor := pdw.DirectoryWalker.directoryWalker.DirectoryVisitor
	pdw.DirectoryWalker.directoryWalker.DirectoryVisitor = func(path Dirname, d fs.DirEntry, err error) error {
		pdw.Progress.value.Add(1)
		return baseVisitor(path, d, err)
	}

	return pdw
}

// This is the struct that informs the progress bar of totals and current value
// It exists to fulfill the Progressable interface
type directoryWalkerProgress struct {
	total        atomic.Int64
	value        atomic.Int64
	finishedChan chan struct{}
}

func NewDirectoryWalkerProgress(path string) *directoryWalkerProgress {

	dw := &directoryWalkerProgress{
		finishedChan: make(chan struct{}),
	}
	return dw
}
func (dw *directoryWalkerProgress) Done() {
	close(dw.finishedChan)
}

// Progress interface methods
func (dw *directoryWalkerProgress) Total() int64 {
	return dw.total.Load()
}
func (dw *directoryWalkerProgress) Value() int64 {
	return dw.value.Load()
}
func (dw *directoryWalkerProgress) FinishedChan() <-chan struct{} {
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
	// FIXME this could live in a go routine with a waitgroup
	if err := pdw.dirCount.Walk(root); err != nil {
		pdw.Progress.Done()
		return err
	}

	// Second pass: real traversal with progress increments
	err := pdw.DirectoryWalker.Walk(root)
	pdw.Progress.Done()
	return err
}

// Ensure directoryWalkerProgress implements Progressable
var _ Progressable = (*directoryWalkerProgress)(nil)
