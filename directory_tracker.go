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

type finishedB uint32

func (f *finishedB) Get() bool {
	return atomic.LoadUint32((*uint32)(f)) > 0
}
func (f *finishedB) Set() {
	atomic.StoreUint32((*uint32)(f), 1)
}

type DirTracker struct {
	directoryCountTotal   int64
	directoryCountVisited int64
	// We do not lock the dm map as we only access it in a single threaded manner
	// i.e. only the directory walker or things it calls have access
	dm        map[string]DirectoryTrackerInterface
	newEntry  func(dir string) (DirectoryTrackerInterface, error)
	lastPath  lastPath
	tokenChan chan struct{}
	wg        *sync.WaitGroup
	errChan   chan error

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
func NewDirTracker(dir string, newEntry func(string) (DirectoryTrackerInterface, error)) *DirTracker {
	numOutsanding := NumTrackerOutstanding // FIXME expose this
	var dt DirTracker
	dt.dm = make(map[string]DirectoryTrackerInterface)
	dt.newEntry = newEntry
	dt.tokenChan = makeTokenChan(numOutsanding)
	dt.wg = new(sync.WaitGroup)
	dt.errChan = make(chan error)
	dt.wg.Add(1)
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
func (dt *DirTracker) runChild(de DirectoryTrackerInterface) {
	// Start is allowed to consume significant time
	// In fact it may directly be the main runner
	err := de.Start()
	if err != nil {
		dt.errChan <- err
	}
	dt.wg.Done()
}
func (dt *DirTracker) serviceChild(de DirectoryTrackerInterface) {
	for err := range de.ErrChan() {
		if err != nil {
			dt.errChan <- err
		}
	}
	dt.wg.Done()
}

// Should we export this?
// so that clients can not have to recreate them
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

func (dt *DirTracker) populateDircount(dir string) {
	defer dt.wg.Done()
	err := filepath.WalkDir(dir, dt.directoryWalkerPopulateDircount)
	if err != nil {
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
		atomic.AddInt64(&dt.directoryCountTotal, 1)
	}
	_, file := filepath.Split(path)
	if file == ".mdSkipDir" {
		return filepath.SkipDir
	}
	return nil
}
func (dt *DirTracker) directoryWalker(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if d.IsDir() {
		if isHiddenDirectory(path) {
			return filepath.SkipDir
		}
		atomic.AddInt64(&dt.directoryCountVisited, 1)
		closerFunc := func(pt string) {
			// FIXME wewill want this back when we are not revisiting
			// de, ok := dt.dm[pt]
			// if ok {
			// 	de.Close()
			// }
			// delete(dt.dm, pt)
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

	<-dt.tokenChan
	callback := func() {
		dt.tokenChan <- struct{}{}
	}

	de, err := dt.getDirectoryEntry(dir)
	if err != nil {
		return fmt.Errorf("%w::%s", err, path)
	}
	if de == nil {
		return fmt.Errorf("%w::%s", errorMissingDe, path)
	}
	de.VisitFile(dir, file, d, callback)
	return nil
}

func (dt *DirTracker) Revisit(dir string, dirVisitor func(dir string) error, fileVisitor func(dm DirectoryEntryInterface, dir, fn string, fileStruct FileStruct) error) {
	for path, de := range dt.dm {
		if dirVisitor != nil {
			dirVisitor(path)
		}
		de.Revisit(path, fileVisitor)
	}
}

type dirTrackerJob struct {
	dir string
	mf  func(string) (DirectoryTrackerInterface, error)
}

// func runParallelDirTrackerJob(jobs []dirTrackerJob, registerChanger func(*DirTracker) <-chan error {
// 	errChan := make(chan error)
// 	var wg sync.WaitGroup
// 	wg.Add(len(jobs))
// 	for _, job := range jobs {
// 		go func(job dirTrackerJob) {
// 			for err := range NewDirTracker(job.dir, job.mf) {
// 				if err != nil {
// 					errChan <- err
// 				}
// 			}
// 			wg.Done()
// 		}(job)
// 	}
// 	go func() {
// 		wg.Wait()
// 		close(errChan)
// 	}()
// 	return errChan
// }

func runSerialDirTrackerJob(jobs []dirTrackerJob, registerChanger func(*DirTracker)) <-chan error {
	errChan := make(chan error)
	go func() {
		for _, job := range jobs {
			ndt := NewDirTracker(job.dir, job.mf)
			if registerChanger != nil {
				registerChanger(ndt)
			}
			for err := range ndt.ErrChan() {
				if err != nil {
					errChan <- err
				}
			}
		}
		close(errChan)
	}()
	return errChan
}
