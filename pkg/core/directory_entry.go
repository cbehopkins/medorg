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
type DirectoryVisitorFunc func(dm DirectoryEntryInterface, directory string, file string, d fs.DirEntry) error

// DirectoryEntryInterface any directory object needs to support this
type DirectoryEntryInterface interface {
	Persist(string) error
	Visitor(directory, file string, d fs.DirEntry) error
	Revisit(dir string, visitor func(dm DirectoryEntryInterface, directory string, file string, fileStruct FileStruct) error) error
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
// Upon creation it will open the appropriate directory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems   chan workItem
	closeChan   chan struct{}
	Dir         string
	errorChan   chan error // now buffered
	Dm          DirectoryEntryInterface
	activeFiles *sync.WaitGroup
	closed      *atomic.Bool
}

// NewDirectoryEntry creates a directory entry
// That is an entry for each file in the dirctory
// We will later be visited populating this structure
func NewDirectoryEntry(path string, mkF EntryMaker) (DirectoryEntry, error) {
	var itm DirectoryEntry
	var err error
	itm.Dir = path
	itm.Dm, err = mkF(path)
	if err != nil {
		return itm, err
	}

	itm.workItems = make(chan workItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error, 4) // buffered to avoid blocking
	itm.activeFiles = new(sync.WaitGroup)
	itm.closed = &atomic.Bool{}
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
		return // Already closed
	}
	close(de.closeChan)
	close(de.workItems) // Safe: atomic.Bool ensures only one Close() runs
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
		case wi, ok := <-de.workItems:
			if !ok {
				// workItems closed (by Close()), finish up
				de.activeFiles.Wait()
				select {
				case de.errorChan <- de.Dm.Persist(de.Dir):
				default:
				}
				return
			}
			go func(dir, file string, d fs.DirEntry) {
				err := de.Dm.Visitor(dir, file, d)
				// Non-blocking send - if channel full, error is dropped
				// This prevents deadlock if errorChan gets backed up
				select {
				case de.errorChan <- err:
				default:
				}
				wi.callback()
				de.activeFiles.Done()
			}(wi.dir, wi.file, wi.d)
		case <-de.closeChan:
			// closeChan signals us to finish, but Close() already closed workItems
			// so the next iteration will hit the !ok case and exit
			// Just drain any remaining items that were sent before workItems closed
			for wi := range de.workItems {
				go func(dir, file string, d fs.DirEntry, callback func()) {
					err := de.Dm.Visitor(dir, file, d)
					// Non-blocking send - prevent deadlock if channel full
					select {
					case de.errorChan <- err:
					default:
					}
					callback()
					de.activeFiles.Done()
				}(wi.dir, wi.file, wi.d, wi.callback)
			}
			de.activeFiles.Wait()
			select {
			case de.errorChan <- de.Dm.Persist(de.Dir):
			default:
			}
			return
		}
	}
}

func (de DirectoryEntry) Revisit(dir string, visitor func(dm DirectoryEntryInterface, directory string, file string, fileStruct FileStruct) error) error {
	return de.Dm.Revisit(dir, visitor)
}
