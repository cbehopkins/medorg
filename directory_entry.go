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
type DirectoryVisitorFunc func(de DirectoryEntry, directory string, file string, d fs.DirEntry) error

// DirectoryEntry represents a single directory
// Upon creation it will open the appropriate direxctory's (md5)
// xml file, and when requested, close it again
// We are also able to send it files to work
type DirectoryEntry struct {
	workItems   chan WorkItem
	closeChan   chan struct{}
	fileWorker  DirectoryVisitorFunc
	dir         string
	errorChan   chan error
	dm          DirectoryMap
	activeFiles *sync.WaitGroup
	perFunc     func() error
}

func NewDirectoryEntry(path string, fw DirectoryVisitorFunc) DirectoryEntry {
	var itm DirectoryEntry
	itm.dir = path
	itm.workItems = make(chan WorkItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error)
	itm.fileWorker = fw
	// TBD, can we go this somehow? Do we even need to if we read it in quick enough?
	// FIXME error prop
	itm.dm, _ = DirectoryMapFromDir(path)
	itm.activeFiles = new(sync.WaitGroup)
	itm.activeFiles.Add(1) // need to dummy add 1 to get it going
	itm.perFunc = func() error { return itm.dm.WriteDirectory(path) }
	go itm.worker()
	return itm // I think here we should return the worker function for the receiver to go. So that they can mutate the itm themselves before starting it
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

func (de DirectoryEntry) worker() {
	defer close(de.errorChan)

	// allow file paths to be sent to us for processing
	for {
		select {
		case wi := <-de.workItems:
			go func(dir, file string, d fs.DirEntry) {
				if de.fileWorker != nil {
					de.errorChan <- de.fileWorker(de, dir, file, d)
				}
				de.activeFiles.Done()
				wi.callback()
			}(wi.dir, wi.file, wi.d)
		case <-de.closeChan:
			de.activeFiles.Done() // From the NewDirectoryEntry
			close(de.workItems)
			de.activeFiles.Wait()
			de.errorChan <- de.perFunc()
			return
		}
	}
}

// func (de DirectoryEntry) persist() error {
// 	return de.dm.WriteDirectory(de.dir)
// }

// UpdateValues in the DirectoryEntry to those found on the fs
func (dm DirectoryMap) UpdateValues(directory string, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return err
	}
	file := d.Name()
	fs, ok := dm.Get(file)

	if !ok {
		fsp, err := NewFileStructFromStat(directory, file, info)
		if err != nil {
			return err
		}
		dm.Add(*fsp)
		return nil
	}
	if changed, err := fs.Changed(info); !changed {
		return err
	}
	fs.Mtime = info.ModTime().Unix()
	fs.Size = info.Size()
	fs.Checksum = "" // FIXME should we calculate this
	dm.Add(fs)
	return nil
}

// Used bu one of the mains
func (de DirectoryEntry) DeleteMissingFiles() error {
	return de.dm.DeleteMissingFiles()
}
