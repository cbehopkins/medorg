package medorg

import (
	"io/fs"
	"sync"
)

type workItem struct {
	dir      string
	file     string
	d        fs.DirEntry
	callback func()
}

// DirectoryVisitorFunc implement one of these and it will be called per file
type DirectoryVisitorFunc func(dm DirectoryEntryInterface, directory string, file string, d fs.DirEntry) error

// DirectoryEntryInterface any directory object needs to support this
type DirectoryEntryInterface interface {
	Persist(string) error
	Visitor(directory, file string, d fs.DirEntry) error
	Revisit(dir string, visitor func(dm DirectoryEntryInterface, directory string, file string, fileStruct FileStruct) error)
}

// DirectoryEntryJournalableInterface if you want to store all info
// to a journal, support this
type DirectoryEntryJournalableInterface interface {
	DirectoryEntryInterface
	ToXML(dir string) (output []byte, err error)
	FromXML(input []byte) (dir string, err error)
	Equal(DirectoryEntryInterface) bool
	Len() int
	Copy() DirectoryEntryJournalableInterface
}

// EntryMaker function to make a DirectoryEntry
type EntryMaker func(string) (DirectoryEntryInterface, error)

// DirectoryEntry represents a single directory
// Upon creation it will open the appropriate direxctory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems   chan workItem
	closeChan   chan struct{}
	dir         string
	errorChan   chan error
	dm          DirectoryEntryInterface
	activeFiles *sync.WaitGroup
}

// NewDirectoryEntry creates a directory entry
// That is an entry for each file in the dirctory
// We will later be visited populating this structure
func NewDirectoryEntry(path string, mkF EntryMaker) (DirectoryEntry, error) {
	var itm DirectoryEntry
	var err error
	itm.dir = path
	itm.dm, err = mkF(path)
	if err != nil {
		return itm, err
	}

	itm.workItems = make(chan workItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error)
	itm.activeFiles = new(sync.WaitGroup)
	return itm, nil
}

// ErrChan returns a channel that will have any errors encountered
// the channel closing says that this DE is finished with
func (de DirectoryEntry) ErrChan() <-chan error {
	return de.errorChan
}

// Close the directory
func (de DirectoryEntry) Close() {
	close(de.closeChan)
}

// VisitFile satisfy the DirectoryTrackerInterface
// this type is visiting this file
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
	// Random thought: Could this test if the worker has been started, ad start if needed?
	de.activeFiles.Add(1)
	de.workItems <- workItem{dir, file, d, callback}
}

// Start and run the worker
func (de DirectoryEntry) Start() error {
	de.worker()
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
				wi.callback()
				de.activeFiles.Done()
			}(wi.dir, wi.file, wi.d)
		case <-de.closeChan:
			close(de.workItems)
			de.activeFiles.Wait()
			de.errorChan <- de.dm.Persist(de.dir)
			return
		}
	}
}
func (de DirectoryEntry) Revisit(dir string, visitor func(dm DirectoryEntryInterface, directory string, file string, fileStruct FileStruct) error) {
	de.dm.Revisit(dir, visitor)
}
