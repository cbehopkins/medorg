package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
)

type DirectoryTrackerInterface interface {
	Close(directory string) <-chan error
	// You must call the callback after you have finished whatever you are doing that might be
	// resource intensive.
	VisitFile(dir, file string, d fs.DirEntry, callback func())
}

type DirTracker struct {
	dm        map[string]DirectoryTrackerInterface
	newEntry  func(dir string) DirectoryTrackerInterface
	lastPath  string
	tokenChan chan struct{}
}

func NewDirTracker(dir string, newEntry func(string) DirectoryTrackerInterface) <-chan error {
	errChan := make(chan error)
	numOutsanding := 1 // FIXME expose this
	go func() {
		var dt DirTracker
		dt.dm = make(map[string]DirectoryTrackerInterface)
		dt.newEntry = newEntry
		dt.tokenChan = make(chan struct{}, numOutsanding)
		for i := 0; i < numOutsanding; i++ {
			dt.tokenChan <- struct{}{}
		}

		err := filepath.WalkDir(dir, dt.directoryWalker)
		if err != nil {
			errChan <- err
		}
		for err := range dt.close() {
			if err != nil {
				errChan <- err
			}
		}
		close(errChan)
	}()
	return errChan
}

func (dt *DirTracker) pathCloser(path string) {
	// The job of this is to work out if we have gone out of scope
	// i.e. close /fred/bob if we have received /fred/steve
	// but do not close /fred or /fred/bob when we receive /fred/bob/steve
	// But also, not doing anything is fine!
	dt.lastPath = path
}
func (dt *DirTracker) directoryWalker(path string, d fs.DirEntry, err error) error {
	//fmt.Println("Path:", path, d.Name(), d.IsDir())
	if err != nil {
		return err
	}
	if d.IsDir() {
		_, ok := dt.dm[path]
		if ok {
			return errors.New("descending into a dirctory, that we already have an entry for")
		}

		fmt.Println("Into directory:", path)
		dt.pathCloser(path)
		dt.dm[path] = dt.newEntry(path)
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

	_, ok := dt.dm[dir]

	if !ok {
		return errors.New("missing directory when evaluating path")
	}
	<-dt.tokenChan
	callback := func() {
		dt.tokenChan <- struct{}{}
	}
	dt.dm[dir].VisitFile(dir, file, d, callback)
	return nil
}

func (dt DirTracker) close() <-chan error {
	errorChan := make(chan error)
	go func() {
		for path := range dt.dm {
			for err := range dt.dm[path].Close(path) {
				errorChan <- err
			}
		}
		close(errorChan)
	}()
	return errorChan
}
