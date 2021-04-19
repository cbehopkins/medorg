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
	go itm.worker()
	return itm
}
func (de DirectoryEntry) ErrChan() <-chan error {
	return de.errorChan
}
func (de DirectoryEntry) Close() {
	close(de.closeChan)
}
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
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
					err := de.fileWorker(de, dir, file, d)
					if err != nil {
						de.errorChan <- err
					}
				}
				de.activeFiles.Done()
				wi.callback()
			}(wi.dir, wi.file, wi.d)
		case <-de.closeChan:
			de.activeFiles.Done() // From the NewDirectoryEntry
			close(de.workItems)
			de.activeFiles.Wait()
			err := de.persist()
			if err != nil {
				de.errorChan <- err
			}
			return
		}
	}

}
func (de DirectoryEntry) persist() error {
	return de.dm.WriteDirectory(de.dir)
}

// UpdateValues in the DirectoryEntry to those found on the fs
// FIXME this should be on the Directory Map
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
		fs = *fsp
		dm.Add(fs)
		return nil
	}
	if changed, err := fs.Changed(info); !changed {
		return err
	}
	fs.Mtime = info.ModTime().Unix()
	fs.Size = info.Size()
	fs.Checksum = ""
	dm.Add(fs)
	return nil
}

// Used bu one of the mains
func (de DirectoryEntry) DeleteMissingFiles() error {
	return de.dm.DeleteMissingFiles()
}
