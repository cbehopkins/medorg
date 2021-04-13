package medorg

import (
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
)

type DirectoryTrackerInterface interface {
	ErrChan() <-chan error
	Close()
	// You must call the callback after you have finished whatever you are doing that might be
	// resource intensive.
	VisitFile(dir, file string, d fs.DirEntry, callback func())
}

type DirTracker struct {
	dm        map[string]DirectoryTrackerInterface
	newEntry  func(dir string) DirectoryTrackerInterface
	lastPath  string
	tokenChan chan struct{}
	wg        *sync.WaitGroup
	errChan   chan error
}

func NewDirTracker(dir string, newEntry func(string) DirectoryTrackerInterface) <-chan error {
	numOutsanding := 1 // FIXME expose this
	var dt DirTracker
	dt.dm = make(map[string]DirectoryTrackerInterface)
	dt.newEntry = newEntry
	dt.tokenChan = make(chan struct{}, numOutsanding)
	dt.wg = new(sync.WaitGroup)
	dt.errChan = make(chan error)
	go func() {
		for i := 0; i < numOutsanding; i++ {
			dt.tokenChan <- struct{}{}
		}
		err := filepath.WalkDir(dir, dt.directoryWalker)
		if err != nil {
			dt.errChan <- err
		}
		dt.close()
		dt.wg.Wait()
		close(dt.errChan)
	}()
	return dt.errChan
}

func (dt *DirTracker) pathCloser(path string) {
	// The job of this is to work out if we have gone out of scope
	// i.e. close /fred/bob if we have received /fred/steve
	// but do not close /fred or /fred/bob when we receive /fred/bob/steve
	// But also, not doing anything is fine!
	dt.lastPath = path
}
func (dt DirTracker) getDirectoryEntry(path string) DirectoryTrackerInterface {
	de, ok := dt.dm[path]
	if ok {
		return de
	}
	// Call out to the external function to return a new entry
	de = dt.newEntry(path)
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
	return de
}

func (dt *DirTracker) directoryWalker(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if d.IsDir() {
		dt.pathCloser(path)
		_ = dt.getDirectoryEntry(path)
		return nil
	}
	dir, file := filepath.Split(path)
	if dir == "" {
		dir = "."
	} else {
		// We would:
		// dir = strings.TrimSuffix(dir, "/")
		// but since we always have this suffix(Thanks filepath!), this is faster:
		dir = dir[:len(dir)-1]
	}

	// Bob, add accessor here
	_, ok := dt.dm[dir]

	if !ok {
		return errors.New("missing directory when evaluating path")
	}
	<-dt.tokenChan
	callback := func() {
		dt.tokenChan <- struct{}{}
	}
	// Bob, add accessor here
	dt.dm[dir].VisitFile(dir, file, d, callback)
	return nil
}

func (dt DirTracker) close() {
	for key, val := range dt.dm {
		val.Close()
		delete(dt.dm, key)
	}
}

type dirTrackerJob struct {
	dir string
	mf  func(string) DirectoryTrackerInterface
}

func runParallelDirTrackerJob(jobs []dirTrackerJob) <-chan error {
	errChan := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(jobs))
	for _, job := range jobs {
		go func(job dirTrackerJob) {
			for err := range NewDirTracker(job.dir, job.mf) {
				if err != nil {
					errChan <- err
				}
			}
			wg.Done()
		}(job)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	return errChan
}

func runSerialDirTrackerJob(jobs []dirTrackerJob) <-chan error {
	errChan := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(jobs))
	go func() {
		for _, job := range jobs {
			for err := range NewDirTracker(job.dir, job.mf) {
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
