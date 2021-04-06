package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

type DirectoryRecordInterface interface {
	NewDirectory(directory string) DirectoryRecordInterface
	CloseDirectory(directory string) <-chan error
	VisitFile(dir, file string, d fs.DirEntry)
}

type DirTracker struct {
	dm       map[string]DirectoryRecordInterface
	baseType DirectoryRecordInterface
	lastPath string
}

func NewDirTracker(baseType DirectoryRecordInterface) (dt DirTracker) {
	dt.dm = make(map[string]DirectoryRecordInterface)
	dt.baseType = baseType
	return
}

func (dt *DirTracker) pathCloser(path string) {
	// The job of this is to work out if we have gone out of scope
	// i.e. close /fred/bob if we have received /fred/steve
	// but do not close /fred or /fred/bob when we receive /fred/bob/steve
	// But also, not doing anything is fine!
	dt.lastPath = path
}
func (dt *DirTracker) DirectoryWalker(path string, d fs.DirEntry, err error) error {
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
		dt.dm[path] = dt.baseType.NewDirectory(path)
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
	dt.dm[dir].VisitFile(dir, file, d)
	return nil
}

// func (dt DirTracker) processSingleFile(dir, file string, d fs.DirEntry) {
// 	dt.dm[dir].workItems <- WorkItem{dir, file, d}
// }
func (dt DirTracker) Close() <-chan error {
	errorChan := make(chan error)
	go func() {
		for path := range dt.dm {
			for err := range dt.dm[path].CloseDirectory(path) {
				errorChan <- err
			}
		}
		close(errorChan)
	}()
	return errorChan
}

type WorkItem struct {
	dir  string
	file string
	d    fs.DirEntry
}

// DirectoryEntry represents a single directory
// Upon creation it will open the appropriate direxctory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems  chan WorkItem
	closeChan  chan struct{}
	fileWorker func(string, string, fs.DirEntry)
	dir        string
	errorChan  chan error
}

func (de DirectoryEntry) NewDirectory(directory string) DirectoryRecordInterface {
	return NewDirectoryEntry(directory, nil)
}
func (de DirectoryEntry) CloseDirectory(directory string) <-chan error {
	return de.Close()
}
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry) {
	de.workItems <- WorkItem{dir, file, d}
}
func NewDirectoryEntry(path string, fw func(string, string, fs.DirEntry)) DirectoryEntry {
	itm := new(DirectoryEntry)
	itm.dir = path
	itm.workItems = make(chan WorkItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error)
	itm.fileWorker = fw
	go itm.worker()
	return *itm
}
func (de DirectoryEntry) Close() <-chan error {
	fmt.Println("Closing entry:", de.dir)
	close(de.closeChan)
	return de.errorChan
}
func (de DirectoryEntry) worker() {
	var activeFiles sync.WaitGroup
	defer close(de.errorChan)
	// Read in the directory's Xml (or create it)

	// Then allow file paths to be sent to us for processing
	for {
		select {
		case wi := <-de.workItems:
			activeFiles.Add(1)
			go func(dir, file string, d fs.DirEntry) {
				if de.fileWorker != nil {
					de.fileWorker(dir, file, d)
				}
				activeFiles.Done()
			}(wi.dir, wi.file, wi.d)
		case <-de.closeChan:
			activeFiles.Wait()
			err := de.persist()
			if err != nil {
				de.errorChan <- err
			}
			return
		}
	}

}
func (de DirectoryEntry) persist() error {
	// Save self to disk
	// open the file path
	// render the xml
	// save it to disk
	return nil
}

func main() {

	dt := NewDirTracker(DirectoryEntry{})

	err := filepath.WalkDir(".", dt.DirectoryWalker)
	fmt.Println("Finished walking")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for err := range dt.Close() {
		fmt.Println("Error received on closing:", err)
		os.Exit(2)
	}
}
