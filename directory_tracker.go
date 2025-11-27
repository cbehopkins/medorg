package medorg

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
	Revisit(dir string, visitor func(dm DirectoryEntryInterface, directory string, file string, fileStruct FileStruct) error)
}

type finishedB struct {
	cnt uint32
	fc  chan struct{}
}

func (f *finishedB) Get() bool {
	return atomic.LoadUint32(&f.cnt) > 0
}
func (f *finishedB) Set() {
	close(f.fc)
	atomic.StoreUint32(&f.cnt, 1)
}
func (f *finishedB) Clear() {
	f.fc = make(chan struct{})
	atomic.StoreUint32(&f.cnt, 0)
}

type DirTracker struct {
	directoryCountTotal   int64
	directoryCountVisited int64
	// We do not lock the dm map as we only access it in a single threaded manner
	// i.e. only the directory walker or things it calls have access
	dm              map[string]DirectoryTrackerInterface
	newEntry        func(dir string) (DirectoryTrackerInterface, error)
	lastPath        lastPath
	tokenChan       chan struct{}
	wg              *sync.WaitGroup
	errChan         chan error
	preserveStructs bool

	finished finishedB
}

func makeTokenChan(numOutsanding int) chan struct{} {
	tokenChan := make(chan struct{}, numOutsanding)
	for i := 0; i < numOutsanding; i++ {
		tokenChan <- struct{}{}
	}
	return tokenChan
}

const NumTrackerOutstanding = 4

// NewDirTracker does what it says
// a dir tracker will walk the supplied directory
// for each directory it finds on its walk it will create a newEntry
// That new entry will then have its visitor called for each file in that directory
// At some later time, we will then close the directory
// There are no guaranetees about when this will happen
func NewDirTracker(preserveStructs bool, dir string, newEntry func(string) (DirectoryTrackerInterface, error)) *DirTracker {
	numOutsanding := NumTrackerOutstanding // FIXME expose this
	var dt DirTracker
	dt.dm = make(map[string]DirectoryTrackerInterface)
	dt.newEntry = newEntry
	dt.tokenChan = makeTokenChan(numOutsanding)
	dt.wg = new(sync.WaitGroup)
	dt.errChan = make(chan error)
	dt.wg.Add(1) // add one for populateDircount
	dt.finished.Clear()
	dt.preserveStructs = preserveStructs
	go dt.populateDircount(dir)
	go func() {
		err := filepath.WalkDir(dir, dt.directoryWalker)
		if err != nil {
			dt.errChan <- err
		}
		for _, val := range dt.dm {
			val.Close()
		}
		dt.wg.Wait()
		if dt.Total() != dt.Value() {
			// FIXME I'm not sure panic is correct here
			// If the file system changes while we are walking, we may not get the correct count
			// Helpful for debugging though
			panic("hadn't actually finished")
		}
		dt.finished.Set()
		close(dt.errChan)
		close(dt.tokenChan)
	}()

	return &dt
}

// ErrChan - returns any errors we encounter
// We retuyrn as a channel as we don't stop on *most* errors
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

// Finished Channel - have we finished yet?
func (dt *DirTracker) FinishedChan() <-chan struct{} {
	return dt.finished.fc
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
	// Fast path - does it already exist? If so, use it!
	de, ok := dt.dm[path]
	if ok && de != nil {
		return de, nil
	}
	// Call out to the external function to return a new entry
	de, err := dt.newEntry(path)
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
		// FIXME Question: I did eveything else on this with atomics - is this correct?
		dt.directoryCountTotal = -1
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
		// FIXME we will want this back when we are not revisiting
		de, ok := dt.dm[pt]
		if ok {
			de.Close()
		}
		delete(dt.dm, pt)
	}
	if dt.preserveStructs {
		closerFunc = nil
	}
	dt.lastPath.Closer(path, closerFunc)
	de, err := dt.getDirectoryEntry(path)
	if err != nil {
		return fmt.Errorf("%w::%s", err, path)
	}
	if de == nil {
		return fmt.Errorf("%w::%s", errorMissingDe, path)
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
		// We would:
		// dir = strings.TrimSuffix(dir, "/")
		// but since we always have this suffix(Thanks filepath!), this is faster:
		dir = dir[:len(dir)-1]
	}

	// Grab an IO token
	<-dt.tokenChan
	returnToken := func() {
		dt.tokenChan <- struct{}{}
	}

	de, err := dt.getDirectoryEntry(dir)
	if err != nil {
		return fmt.Errorf("%w::%s", err, path)
	}
	if de == nil {
		return fmt.Errorf("%w::%s", errorMissingDe, path)
	}
	de.VisitFile(dir, file, d, returnToken)
	return nil
}

// Revisit allows you to walk through an existing structure
func (dt *DirTracker) Revisit(
	dir string,
	dirVisitor func(dt *DirTracker),
	fileVisitor func(dm DirectoryEntryInterface, dir, fn string, fileStruct FileStruct) error,
	closer <-chan struct{},
) {
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
					log.Println("Revisit saw a closer")
					return
				}
			default:
			}
		}
		atomic.AddInt64(&dt.directoryCountVisited, 1)
		de.Revisit(path, fileVisitor)
	}
}
