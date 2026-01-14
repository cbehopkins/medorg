package core

import (
	"io/fs"
	"sync"
	"sync/atomic"
)

type workItem struct {
	dir      string
	file     string
	d        fs.DirEntry
	callback func()
}

// DirectoryVisitorFunc implement one of these and it will be called per file
// type DirectoryVisitorFunc func(dm DirectoryEntryInterface, directory string, file string, d fs.DirEntry) error

// DirectoryEntryInterface any directory object needs to support this
type DirectoryEntryInterface interface {
	Persist(Dirname) error
	Visitor(path Fpath, d fs.DirEntry) error
}

// DirectoryEntryJournalableInterface if you want to store all info
// to a journal, support this
type DirectoryEntryJournalableInterface interface {
	DirectoryEntryInterface
	ToXML(dir Dirname) (output []byte, err error)
	FromXML(input []byte) (dir Dirname, err error)
	Equal(DirectoryEntryInterface) bool
	Len() int
	Copy() DirectoryEntryJournalableInterface
}

// EntryMaker function to make a DirectoryEntry
type EntryMaker func(string) (DirectoryEntryInterface, error)

// DirectoryEntry represents a single directory
// Upon creation it will open the appropriate directory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems         chan workItem
	Dir               string
	errorChan         chan error // now buffered
	Dm                DirectoryEntryInterface
	activeFiles       *sync.WaitGroup
	closed            *atomic.Bool
	fileProcessTokens chan struct{} // Global concurrency limiter across all directories
}

// NewDirectoryEntry creates a directory entry
// That is an entry for each file in the dirctory
// We will later be visited populating this structure
func NewDirectoryEntry(path string, mkF EntryMaker) (DirectoryEntry, error) {
	return NewDirectoryEntryWithTokens(path, mkF, nil)
}

// NewDirectoryEntryWithTokens creates a directory entry with optional token-based concurrency control
func NewDirectoryEntryWithTokens(path string, mkF EntryMaker, fileProcessTokens chan struct{}) (DirectoryEntry, error) {
	var itm DirectoryEntry
	var err error
	itm.Dir = path
	itm.Dm, err = mkF(path)
	if err != nil {
		return itm, err
	}

	itm.workItems = make(chan workItem)
	itm.errorChan = make(chan error, 4) // buffered to avoid blocking
	itm.activeFiles = new(sync.WaitGroup)
	itm.closed = &atomic.Bool{}
	itm.fileProcessTokens = fileProcessTokens
	return itm, nil
}

// ErrChan returns a channel that will have any errors encountered
// the channel closing says that this DE is finished with
func (de DirectoryEntry) ErrChan() <-chan error {
	return de.errorChan
}

// Close the directory
func (de DirectoryEntry) Close() {
	// Use atomic CAS to ensure we only close once
	if !de.closed.CompareAndSwap(false, true) {
		panic("DirectoryEntry Close called multiple times")
		return // Already closed
	}
	close(de.workItems)
}

// VisitFile satisfy the DirectoryTrackerInterface
// this type is visiting this file
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
	// Random thought: Could this test if the worker has been started, and start if needed?
	de.activeFiles.Add(1)
	de.workItems <- workItem{dir, file, d, callback}
}

// Start and run the worker
func (de DirectoryEntry) Start() error {
	go de.worker()
	return nil
}

func (de DirectoryEntry) worker() {
	// allow file paths to be sent to us for processing
	for wi := range de.workItems {
		// Acquire token from global pool if available (before spawning goroutine)
		if de.fileProcessTokens != nil {
			<-de.fileProcessTokens // Block until token available
		}
		go func(dir, file string, d fs.DirEntry) {
			// Release token when done
			defer func() {
				if de.fileProcessTokens != nil {
					de.fileProcessTokens <- struct{}{}
				}
			}()
			err := de.Dm.Visitor(NewFpath(dir, file), d)
			de.errorChan <- err
			wi.callback()
			de.activeFiles.Done()
		}(wi.dir, wi.file, wi.d)
	}
	de.activeFiles.Wait()
	de.errorChan <- de.Dm.Persist(Dirname(de.Dir))
	close(de.errorChan)
}
