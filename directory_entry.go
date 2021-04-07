package medorg

import (
	"fmt"
	"io/fs"
	"sync"
)

type WorkItem struct {
	dir  string
	file string
	d    fs.DirEntry
}
type DirectoryVisitorFunc func(de DirectoryEntry, directory string, file string, d fs.DirEntry)

// DirectoryEntry represents a single directory
// Upon creation it will open the appropriate direxctory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems  chan WorkItem
	closeChan  chan struct{}
	fileWorker DirectoryVisitorFunc
	dir        string
	errorChan  chan error
}

func NewDirectoryEntry(path string, fw DirectoryVisitorFunc) DirectoryEntry {
	itm := new(DirectoryEntry)
	itm.dir = path
	itm.workItems = make(chan WorkItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error)
	itm.fileWorker = fw
	go itm.worker()
	return *itm
}
func (de DirectoryEntry) Close(directory string) <-chan error {
	fmt.Println("Closing entry:", de.dir)
	close(de.closeChan)
	return de.errorChan
}
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry) {
	de.workItems <- WorkItem{dir, file, d}
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
					de.fileWorker(de, dir, file, d)
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
func (de DirectoryEntry) UpdateChecksum(file string) error {
	return nil
}
