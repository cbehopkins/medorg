package core

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	pb "github.com/cbehopkins/pb/v3"
)

type workerPool struct {
	workCh  chan func() error
	errorCh chan error
	wg      sync.WaitGroup
}

func newWorkerPool(workerCount int) *workerPool {
	pool := &workerPool{
		workCh:  make(chan func() error, workerCount*2),
		errorCh: make(chan error, workerCount*2),
	}
	pool.wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer pool.wg.Done()
			for work := range pool.workCh {
				if err := work(); err != nil {
					pool.errorCh <- err
				}
			}
		}()
	}
	return pool
}

func (wp *workerPool) Submit(work func() error) {
	wp.workCh <- work
}
func (wp *workerPool) Close() error {
	close(wp.workCh)
	wp.wg.Wait()
	close(wp.errorCh)
	var err error
	for e := range wp.errorCh {
		if err == nil {
			err = e
		} else {
			err = errors.New(err.Error() + "; " + e.Error())
		}
	}
	return err
}

// Errored returns the first error encountered by any worker, or nil if no errors occurred.
func (wp *workerPool) Errored() error {
	select {
	case err := <-wp.errorCh:
		return err
	default:
		return nil
	}
}

// DirectoryVisitor is a callback function type for visiting directories
// It is passed the Directory paramaters and an []fs.DirEntry for the files
type DirectoryVisitor func(path Dirname, entries []fs.DirEntry, err error) error
type DirectoryWalker struct {
	shouldIgnore     func(path string) bool // Optional function to check if path should be ignored
	WorkTokens       chan struct{}
	DirectoryVisitor DirectoryVisitor
	cancelChan       chan struct{}
	fileVisitors     []ForEachCallback
	fileMutators     []DmMutCallback
	mutatePool       *mutatePool
	workerPool       *workerPool
}

func (dw *DirectoryWalker) AddFileMutator(fm DmMutCallback) {
	dw.fileMutators = append(dw.fileMutators, fm)
}
func (dw *DirectoryWalker) AddFileVisitor(fv ForEachCallback) {
	dw.fileVisitors = append(dw.fileVisitors, fv)
}

func (dw *DirectoryWalker) grabWorkToken() {
	if dw.WorkTokens != nil {
		<-dw.WorkTokens
	}
}
func (dw *DirectoryWalker) releaseWorkToken() {
	if dw.WorkTokens != nil {
		dw.WorkTokens <- struct{}{}
	}
}

// NewDirectoryWalker creates a DirectoryWalker
// You must supply a path to a work token chan.
// Typically you use the same one you already have
func NewDirectoryWalker(WorkTokens chan struct{}) *DirectoryWalker {
	dw := &DirectoryWalker{
		WorkTokens: WorkTokens,
		cancelChan: make(chan struct{}),
		mutatePool: newMutatePool(),
	}
	dw.fileVisitors = make([]ForEachCallback, 0)
	dw.fileMutators = make([]DmMutCallback, 0)
	dw.workerPool = newWorkerPool(runtime.NumCPU() * 4)

	return dw
}
func (dw *DirectoryWalker) Cancel() {
	close(dw.cancelChan)
}

func (dw *DirectoryWalker) Close() error {
	if dw.mutatePool != nil {
		dw.mutatePool.close()
		dw.mutatePool = nil
	}
	if dw.workerPool != nil {
		if err := dw.workerPool.Close(); err != nil {
			return err
		}
		dw.workerPool = nil
	}
	return nil
}

var errSkipFileVisitorRun = errors.New("Skip this file visitor run")

func (dw *DirectoryWalker) Walk(root string) error {
	if err := dw.workerPool.Errored(); err != nil {
		return err
	}
	// FIXME there is an optimisation we could do here
	// that collects up the file entries at the same time as walking
	// This would make doing the checkcalc much faster as we only need a single pass
	// Check it is a dir first
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("Walk root is not a directory")
	}

	// FIXME there's an optimisation here where we
	// can use the os.ReadDir that comes later to get all entries in one go
	// But KISS for now
	if err := dw.shouldSkipDir(root); err != nil {
		// It's not an error, but still, skip it
		return nil
	}

	dw.grabWorkToken()
	entries, err := os.ReadDir(root)
	dw.releaseWorkToken()
	if err != nil {
		return err
	}
	directories := make([]fs.DirEntry, 0)
	files := make([]fs.DirEntry, 0)

	var skipFileVisitors bool
	for _, entry := range entries {
		if entry.IsDir() {
			directories = append(directories, entry)
		} else {
			files = append(files, entry)
		}
	}

	if dw.DirectoryVisitor != nil {
		err = dw.DirectoryVisitor(Dirname(root), entries, nil)
		if err != nil {
			if errors.Is(err, filepath.SkipDir) {
				log.Println("Skipping", root, "and all subdirectories as per SkipDir")
				return nil
			}
			if errors.Is(err, errSkipFileVisitorRun) {
				skipFileVisitors = true
			} else {
				// If it's a filepath.SkipAll then allow the error to work its way up the tree
				log.Println("Aborting due to err in dw.DirectoryVisitor", err)
				return err
			}
		}
	}

	if !skipFileVisitors {
		// Visit this directory, run the visitors etc
		if err := dw.dirVisitor(Dirname(root), entries, nil); err != nil {
			// SkipDir: skip this directory and its subdirectories (return nil to stop descent)
			// SkipAll: stop all directory walking (propagate error up)
			if errors.Is(err, filepath.SkipDir) {
				log.Println("Skipping", root, "and all subdirectories as per SkipDir <- dirVisitor")
				return nil
			}
			return err
		}
	}
	// Now walk sub-directories
	// Maybe use WalkMulti here?
	for _, d := range directories {
		subdirPath := filepath.Join(root, d.Name())
		err := dw.Walk(subdirPath)
		if err != nil {
			if errors.Is(err, filepath.SkipDir) {
				log.Println("Skipping", subdirPath, "and all subdirectories as per SkipDir <- Walk")
				continue
			}
			// If it's a filepath.SkipAll then allow the error to work its way up the tree
			log.Println("SkipAll found at", root, d, "in subdirectory walk")
			return err
		}
	}
	return nil
}

func (dw *DirectoryWalker) shouldSkipDir(path string) error {
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

// Note: this is DirectoryWalker and therefore we will now visit all files in the directory
func (dw *DirectoryWalker) dirVisitor(path Dirname, entries []os.DirEntry, err error) error {
	// log.Printf("dirVisitor called for %s", path)
	dw.grabWorkToken()
	defer dw.releaseWorkToken()

	dm, err := DirectoryMapFromDirEntries(path, entries)
	if err != nil {
		return err
	}
	// Inject the shared worker pool
	dm.pool = dw.mutatePool
	err = dm.ChecksumCalc(dw.WorkTokens)
	if err != nil {
		return err
	}

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
	if len(dw.fileMutators) > 0 {
		err := dm.RangeMutate(func(file Fpath, d os.FileInfo, fs FileStruct) (FileStruct, error) {
			ignoreCounter := 0
			for _, fm := range dw.fileMutators {
				var err error
				fs, err = fm(file, d, fs)
				if errors.Is(err, ErrIgnoreThisMutate) {
					ignoreCounter++
					continue
				}

				if err != nil {
					return fs, err
				}
			}
			// If we get an ignore from all mutators, we skip the file
			//Anyone not saying "Don't mutate" means we need to continue with mutation
			if ignoreCounter == len(dw.fileMutators) {
				return fs, ErrIgnoreThisMutate
			}
			return fs, nil
		})
		if err != nil {
			return err
		}
	}
	// return dm.Persist(path)
	if err := dw.workerPool.Errored(); err != nil {
		return err
	}
	dw.workerPool.Submit(func() error {
		return dm.Persist(path)
	})
	return nil
}

// ProgressableDirectoryWalker extends DirectoryWalker with progress tracking
// Which means we first need to count the number of directories to visit
// Then we can walk the directories updating progress as we go
type ProgressableDirectoryWalker struct {
	DirectoryWalker
	dirCount DirectoryWalker
	Progress *directoryWalkerProgress
}

func NewProgressableDirectoryWalker(WorkTokens chan struct{}, path string) *ProgressableDirectoryWalker {
	dw := NewDirectoryWalker(WorkTokens)
	pdw := &ProgressableDirectoryWalker{
		DirectoryWalker: *dw,
		Progress:        &directoryWalkerProgress{finishedChan: make(chan struct{})},
	}

	// Counting walker shares the same ignore/cancel logic but only increments totals.
	pdw.dirCount = DirectoryWalker{
		WorkTokens: nil, // counting pass is lightweight; no token gate needed
		// This needs to come from the Ctrl-C handler to be effective
		cancelChan:   pdw.DirectoryWalker.cancelChan,
		shouldIgnore: pdw.DirectoryWalker.shouldIgnore,
		mutatePool:   dw.mutatePool,                   // Share the pool to avoid creating temporary ones
		workerPool:   newWorkerPool(runtime.NumCPU()), // Initialize worker pool for counting pass
	}
	pdw.dirCount.DirectoryVisitor = func(path Dirname, entries []os.DirEntry, err error) error {
		pdw.Progress.total.Add(1)
		return errSkipFileVisitorRun
	}

	// Wrap the primary DirectoryVisitor to record progress as directories are processed.
	pdw.DirectoryWalker.DirectoryVisitor = func(path Dirname, entries []os.DirEntry, err error) error {
		pdw.Progress.value.Add(1)
		return nil
	}

	return pdw
}

// Close cleans up resources for both the main walker and the counting walker
func (pdw *ProgressableDirectoryWalker) Close() error {
	// Close the counting walker's worker pool first
	if pdw.dirCount.workerPool != nil {
		if err := pdw.dirCount.workerPool.Close(); err != nil {
			log.Printf("warning: failed to close dirCount workerPool: %v", err)
		}
		pdw.dirCount.workerPool = nil
	}

	// Then close the main DirectoryWalker (which includes its workerPool)
	return pdw.DirectoryWalker.Close()
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
	pdw.dirCount.shouldIgnore = pdw.DirectoryWalker.shouldIgnore
	pdw.dirCount.cancelChan = pdw.DirectoryWalker.cancelChan

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
var _ pb.Progressable = (*directoryWalkerProgress)(nil)
