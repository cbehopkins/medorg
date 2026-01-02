package core

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var errorMissingDe = errors.New("missing de when evaluating directory")

type DirectoryTrackerInterface interface {
	ErrChan() <-chan error
	Start() error
	Close()
	// You must call the callback after you have finished whatever you are doing that might be
	// resource intensive.
	VisitFile(dir, file string, d fs.DirEntry, callback func())
}

// finishedB provides a simple atomic boolean flag for tracking completion state.
// This was previously more complex with a channel-based notification mechanism,
// but the channel was never actually used in the codebase, so we simplified to
// just an atomic boolean. This eliminates the problematic Clear() behavior that
// would create new channels and potentially orphan goroutines waiting on old ones.
type finishedB struct {
	cnt atomic.Uint32
}

func (f *finishedB) Get() bool {
	return f.cnt.Load() > 0
}

func (f *finishedB) Set() {
	f.cnt.Store(1)
}

func (f *finishedB) Clear() {
	f.cnt.Store(0)
}

// DirTracker tracks directories and their entries
// It functions as a cache of DirectoryEntry structures
// That is an entry for each directory we have seen
// It will try and be intelligent about retiring entries when we are done with them
type DirTracker struct {
	directoryCountTotal   int64
	directoryCountVisited int64
	// We do not lock the dm map as we only access it in a single threaded manner
	// i.e. only the directory walker or things it calls have access
	dm              map[string]DirectoryTrackerInterface
	trackerMaker    func(dir string) (DirectoryTrackerInterface, error)
	lastPath        lastPath
	workTokens      chan struct{}
	wg              *sync.WaitGroup
	errChan         chan error
	preserveStructs bool

	finished     finishedB
	finishedChan chan struct{}
}

// MakeTokenChan creates a buffered channel with a fixed number of tokens for concurrency control
func MakeTokenChan(numOutstanding int) chan struct{} {
	tkc := make(chan struct{}, numOutstanding)
	for range numOutstanding {
		tkc <- struct{}{}
	}
	return tkc
}

const NumTrackerOutstanding = 4

// NewDirTracker does what it says
// a dir tracker will walk the supplied directory
// for each directory it finds on its walk it will create a newEntry
// That new entry will then have its visitor called for each file in that directory
// At some later time, we will then close the directory
// There are no guaranetees about when this will happen
func NewDirTracker(preserveStructs bool, dir string, trackerMaker func(string) (DirectoryTrackerInterface, error)) *DirTracker {
	var dt DirTracker
	dt.dm = make(map[string]DirectoryTrackerInterface)
	dt.trackerMaker = trackerMaker
	dt.workTokens = MakeTokenChan(NumTrackerOutstanding)
	dt.wg = new(sync.WaitGroup)
	dt.errChan = make(chan error, 8) // buffered to avoid blocking
	dt.finishedChan = make(chan struct{})
	dt.wg.Add(1) // add one for populateDircount
	dt.finished.Clear()
	dt.preserveStructs = preserveStructs
	// go dt.populateDircount(dir)
	dt.populateDircount(dir) // Removed go for testing purposes
	go func() {
		err := filepath.WalkDir(dir, dt.directoryWalker)
		if err != nil {
			dt.errChan <- err
		}
		for _, val := range dt.dm {
			// Why are we closing their channels?
			val.Close()
		}
		dt.wg.Wait()
		if dt.Total() != dt.Value() {
			dt.errChan <- fmt.Errorf(
				"directory walk incomplete: expected to visit %d directories but only visited %d (filesystem may have changed during walk)",
				dt.Total(), dt.Value())
		}
		dt.finished.Set()
		close(dt.errChan)
		close(dt.workTokens)
		close(dt.finishedChan)
	}()

	return &dt
}

// ErrChan - returns any errors we encounter
// We return as a channel as we don't stop on *most* errors
func (dt *DirTracker) ErrChan() <-chan error {
	return dt.errChan
}

// Total tracks how many items there are to visit
func (dt *DirTracker) Total() int64 {
	return atomic.LoadInt64(&dt.directoryCountTotal)
}

// Value is how far we are though visiting
func (dt *DirTracker) Value() int64 {
	return atomic.LoadInt64(&dt.directoryCountVisited)
}

// Finished - have we finished yet?
func (dt *DirTracker) Finished() bool {
	return dt.finished.Get()
}

// FinishedChan returns a channel that will be closed when the directory tracking is complete
func (dt *DirTracker) FinishedChan() <-chan struct{} {
	return dt.finishedChan
}

func (dt *DirTracker) runChild(de DirectoryTrackerInterface) {
	// Start is allowed to consume significant time
	// In fact it may directly be the main runner
	err := de.Start()
	if err != nil {
		dt.errChan <- err
	}
	dt.wg.Done()
}

// serviceChild - copy errors from the child to the parent
func (dt *DirTracker) serviceChild(de DirectoryTrackerInterface) {
	for err := range de.ErrChan() {
		if err != nil {
			dt.errChan <- err
		}
	}
	dt.wg.Done()
}

// getDirectoryEntry - get a directory entry
// If it doesn't exist, create it
func (dt *DirTracker) getDirectoryEntry(path string) (DirectoryTrackerInterface, error) {
	path = filepath.Clean(path)
	// Fast path - does it already exist? If so, use it!
	de, ok := dt.dm[path]
	if ok && de != nil {
		return de, nil
	}
	// Call out to the external function to return a new entry
	de, err := dt.trackerMaker(path)
	if err != nil {
		return nil, err
	}

	dt.dm[path] = de
	dt.wg.Add(2)
	go dt.runChild(de)
	go dt.serviceChild(de)
	return de, nil
}

// populateDircount - populate the directory count
// i.e. how many directories we have to visit
func (dt *DirTracker) populateDircount(dir string) {
	defer dt.wg.Done()
	err := filepath.WalkDir(dir, dt.directoryWalkerPopulateDircount)
	if err != nil {
		atomic.StoreInt64(&dt.directoryCountTotal, -1)
		return
	}
}

func (dt *DirTracker) directoryWalkerPopulateDircount(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if d.IsDir() {
		if isHiddenDirectory(path) {
			return filepath.SkipDir
		}
		log.Println("populating dir", path, dt.Total())
		atomic.AddInt64(&dt.directoryCountTotal, 1)
	}
	_, file := filepath.Split(path)
	// FIXME I think this logic is broken as this is the first thing it should look for
	// Howevver it could have done a bunch of stuff before hitting this
	if file == ".mdSkipDir" {
		return filepath.SkipDir
	}
	return nil
}

func (dt *DirTracker) handleDirectory(path string) error {
	if isHiddenDirectory(path) {
		return filepath.SkipDir
	}
	log.Println("visiting dir", path, dt.Value(), "of", dt.Total())
	atomic.AddInt64(&dt.directoryCountVisited, 1)
	closerFunc := func(pt string) {
		// TODO: Re-enable closerFunc cleanup when preserveStructs is false. Currently disabled
		// because preserveStructs=true is used during RevisitAll() to keep directory entries
		// alive in memory for potential reuse. When preserveStructs=false (initial walk only),
		// we should call Close() on entries we're done with to free resources (file handles,
		// memory buffers). This is an optimization for single-pass operations where memory
		// efficiency is more important than reusability of directory structures.
		de, ok := dt.dm[pt]
		if ok {
			de.Close()
		}
		delete(dt.dm, pt)
	}
	if dt.preserveStructs {
		closerFunc = nil
	}
	if err := dt.lastPath.Closer(path, closerFunc); err != nil {
		return err
	}
	de, err := dt.getDirectoryEntry(path)
	if err != nil {
		return fmt.Errorf("error getting directory entry for %s: %w", path, err)
	}
	if de == nil {
		return fmt.Errorf("missing directory entry for %s: %w", path, errorMissingDe)
	}
	return nil
}

func (dt *DirTracker) directoryWalker(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if d.IsDir() {
		return dt.handleDirectory(path)
	}
	dir, file := filepath.Split(path)
	if file == ".mdSkipDir" {
		log.Println("Skipping:", dir)
		return filepath.SkipDir
	}
	if dir == "" {
		dir = "."
	} else {
		dir = filepath.Clean(dir)
	}

	// Grab an IO token
	<-dt.workTokens
	returnToken := func() {
		dt.workTokens <- struct{}{}
	}

	de, err := dt.getDirectoryEntry(dir)
	if err != nil {
		return fmt.Errorf("error getting directory entry for %s: %w", path, err)
	}
	if de == nil {
		return fmt.Errorf("missing directory entry for %s: %w", path, errorMissingDe)
	}
	de.VisitFile(dir, file, d, returnToken)
	return nil
}

// RevisitAll allows you to walk through all tracked directories in the DirTracker
func (dt *DirTracker) RevisitAll(
	dir string,
	dirVisitor func(dt *DirTracker),
	fileVisitor func(dm DirectoryEntryInterface, dir, fn string, fileStruct FileStruct) error,
	closer <-chan struct{},
) error {
	dt.finished.Clear()
	defer dt.finished.Set()
	atomic.StoreInt64(&dt.directoryCountVisited, 0)
	if dirVisitor != nil {
		dirVisitor(dt)
	}
	for path, de := range dt.dm {
		if closer != nil {
			select {
			case _, ok := <-closer:
				if !ok {
					log.Println("RevisitAll saw a closer")
					return nil
				}
			default:
			}
		}
		atomic.AddInt64(&dt.directoryCountVisited, 1)
		entry, ok := de.(DirectoryEntry)
		if ok {
			if err := entry.Revisit(path, fileVisitor); err != nil {
				return fmt.Errorf("RevisitAll: revisit failed for %s: %w", path, err)
			}
		} else {
			panic(fmt.Sprintf("RevisitAll: entry for path %s is not of type DirectoryEntry (type: %T) - this is a fundamental design error", path, de))
		}
	}
	return nil
}
