package medorg

import (
	"io/fs"
	"sync"
)

type WorkItem struct {
	dir      string
	file     string
	d        fs.DirEntry
	callback func()
}
type DirectoryVisitorFunc func(dm DirectoryEntryInterface, directory string, file string, d fs.DirEntry) error
type DirectoryEntryInterface interface {
	Persist(string) error
	Visitor(directory, file string, d fs.DirEntry) error
}
type EntryMaker func(string) (DirectoryEntryInterface, error)

// DirectoryEntry represents a single directory
// Upon creation it will open the appropriate direxctory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems   chan WorkItem
	closeChan   chan struct{}
	dir         string
	errorChan   chan error
	dm          DirectoryEntryInterface
	activeFiles *sync.WaitGroup
}

func NewDirectoryEntry(path string, mkF EntryMaker) (DirectoryEntry, error) {
	var itm DirectoryEntry
	var err error
	itm.dir = path
	itm.dm, err = mkF(path)
	if err != nil {
		return itm, err
	}

	itm.workItems = make(chan WorkItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error)
	// TBD, can we go this somehow? Do we even need to if we read it in quick enough?
	itm.activeFiles = new(sync.WaitGroup)
	itm.activeFiles.Add(1) // need to dummy add 1 to get it going
	return itm, nil        // I think here we should return the worker function for the receiver to go. So that they can mutate the itm themselves before starting it
}
func (de DirectoryEntry) ErrChan() <-chan error {
	return de.errorChan
}
func (de DirectoryEntry) Close() {
	close(de.closeChan)
}
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
	// Random thought: Could this test if the worker has been started, ad start if needed?
	de.activeFiles.Add(1)
	de.workItems <- WorkItem{dir, file, d, callback}
}
func (de DirectoryEntry) Start() error {
	go de.worker()
	return nil
}
func (de DirectoryEntry) worker() {
	defer close(de.errorChan)

	// allow file paths to be sent to us for processing
	for {
		select {
		case wi := <-de.workItems:
			go func(dir, file string, d fs.DirEntry) {
				de.errorChan <- de.dm.Visitor(dir, file, d)
				de.activeFiles.Done()
				wi.callback()
			}(wi.dir, wi.file, wi.d)
		case <-de.closeChan:
			de.activeFiles.Done() // From the NewDirectoryEntry
			close(de.workItems)
			de.activeFiles.Wait()
			de.errorChan <- de.dm.Persist(de.dir)
			return
		}
	}
}
