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
}

type DirTracker struct {
	lk                    *sync.Mutex
	dm                    map[string]DirectoryTrackerInterface
	newEntry              func(dir string) (DirectoryTrackerInterface, error)
	lastPath              string
	tokenChan             chan struct{}
	wg                    *sync.WaitGroup
	errChan               chan error
	directoryCountTotal   int64
	directoryCountVisited int64

	finished bool
}

func makeTokenChan(numOutsanding int) chan struct{} {
	tokenChan := make(chan struct{}, numOutsanding)
	for i := 0; i < numOutsanding; i++ {
		tokenChan <- struct{}{}
	}
	return tokenChan
}

const NumTrackerOutstanding = 1

// NewDirTracker does what it says
// a dir tracker will walk the supplied directory
// for each directory it finds on its walk it will create a newEntry
// That new entry will then have its visitor called for each file in that directory
// At some later time, we will then close the directory
// There are no guaranetees about when this will happen
func NewDirTracker(dir string, newEntry func(string) (DirectoryTrackerInterface, error)) *DirTracker {
	numOutsanding := NumTrackerOutstanding // FIXME expose this
	var dt DirTracker
	dt.lk = new(sync.Mutex)
	dt.dm = make(map[string]DirectoryTrackerInterface)
	dt.newEntry = newEntry
	dt.tokenChan = makeTokenChan(numOutsanding)
	dt.wg = new(sync.WaitGroup)
	dt.errChan = make(chan error)
	go func() {
		err := filepath.WalkDir(dir, dt.directoryWalker)
		if err != nil {
			dt.errChan <- err
		}
		dt.close(dir)
		dt.wg.Wait()
		close(dt.errChan)
		close(dt.tokenChan)
		if dt.Total() != dt.Value() {
			panic("Lengths don't match")
		}
	}()
	go dt.populateDircount(dir)
	return &dt
}
func (dt *DirTracker) ErrChan() <-chan error {
	return dt.errChan
}
func (dt *DirTracker) populateDircount(dir string) {
	walker := func(path string, d fs.DirEntry, err error) error {
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

	err := filepath.WalkDir(dir, walker)
	if err != nil {
		dt.directoryCountTotal = -1
		return
	}
}
func (dt *DirTracker) Total() int64 {
	return atomic.LoadInt64(&dt.directoryCountTotal)
}
func (dt *DirTracker) Value() int64 {
	return atomic.LoadInt64(&dt.directoryCountVisited)
}
func (dt *DirTracker) Finished() bool {
	dt.lk.Lock()
	defer dt.lk.Unlock()
	return dt.finished
}

// Should we export this?
// so that clients can not have to recreate them
func (dt *DirTracker) getDirectoryEntry(path string) (DirectoryTrackerInterface, error) {
	de, ok := dt.dm[path]
	if ok && de != nil {
		return de, nil
	}
	// Call out to the external function to return a new entry
	de, err := dt.newEntry(path)
	// FIXME error handling
	if de == nil || err != nil {
		return nil, err
	}
	go func() {
		err := de.Start()
		if err != nil {
			dt.errChan <- err
		}
	}()
	dt.dm[path] = de
	dt.wg.Add(1)
	go func() {
		for err := range de.ErrChan() {
			if err != nil {
				dt.errChan <- err
			}
		}
		dt.wg.Done()
	}()
	return de, nil
}

func (dt DirTracker) getLastPath() string {
	dt.lk.Lock()
	defer dt.lk.Unlock()
	return dt.lastPath
}

func (dt *DirTracker) pathCloser(path string, closerFunc func(string)) {
	// The job of this is to work out if we have gone out of scope
	// i.e. close /fred/bob if we have received /fred/steve
	// but do not close /fred or /fred/bob when we receive /fred/bob/steve
	// But also, not doing anything is fine!

	defer func() {
		dt.lk.Lock()
		dt.lastPath = path
		dt.lk.Unlock()
	}()

	if dt.getLastPath() == "" {
		return
	}
	if closerFunc == nil {
		closerFunc = func(pt string) {
			de, ok := dt.dm[pt]
			if ok {
				de.Close()
			}
		}
	}
	// FIXME make it possbile to select this/another/default to this
	shouldClose := func(pt string) bool {
		isChild, err := isChildPath(pt, dt.getLastPath())
		if err != nil {
			// FIXME
			return false
		}

		return !isChild
	}
	if shouldClose(path) {
		closerFunc(dt.getLastPath())
		delete(dt.dm, dt.getLastPath())
	}
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
		dt.pathCloser(path, nil)
		de, err := dt.getDirectoryEntry(path)
		if err != nil {
			return fmt.Errorf("%w::%s", err, path)
		}
		if de == nil {
			return fmt.Errorf("%w::%s", errorMissingDe, path)
		}
		de, ok := dt.dm[path]
		if !ok {
			log.Fatal("bang!", de)
		}
		return nil
	}
	dir, file := filepath.Split(path)
	if file == ".mdSkipDir" {
		// fmt.Println("Skipping:", dir)
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

func (dt *DirTracker) close(dir string) {
	for key, val := range dt.dm {
		delete(dt.dm, key)
		val.Close()
	}
	dt.lk.Lock()
	dt.finished = true
	dt.lk.Unlock()
}

type dirTrackerJob struct {
	dir string
	mf  func(string) (DirectoryTrackerInterface, error)
}

// func runParallelDirTrackerJob(jobs []dirTrackerJob) <-chan error {
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
	var wg sync.WaitGroup
	wg.Add(len(jobs))
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
			wg.Done()
		}
	}()
	go func() {
		wg.Wait()
		close(errChan)
	}()
	return errChan
}
