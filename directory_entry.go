package medorg

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
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
func (de DirectoryEntry) UpdateValues(d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return err
	}
	file := d.Name()
	fs, ok := de.dm.Get(file)

	if !ok {
		fsp, err := NewFileStructFromStat(de.dir, file, info)
		if err != nil {
			return err
		}
		fs = *fsp
		de.dm.Add(fs)
		return nil
	}
	if changed, err := fs.Changed(info); !changed {
		return err
	}
	fs.Mtime = info.ModTime().Unix()
	fs.Size = info.Size()
	fs.Checksum = ""
	de.dm.Add(fs)
	return nil
}

// SetFs exports the ability to add a filestruct to this directory
func (de DirectoryEntry) SetFs(fs FileStruct) {
	de.dm.Add(fs)
}

// UpdateChecksum will recalc the checksum of an entry
// FIXME this should be on Directory Map
func (de DirectoryEntry) UpdateChecksum(file string, forceUpdate bool) error {
	if Debug && file == "" {
		return errors.New("asked to update a checksum on a null filename")
	}

	fs, ok := de.dm.Get(file)
	if !ok {
		fsp, err := NewFileStruct(de.dir, file)
		if err != nil {
			return nil
		}
		fs = *fsp
		if Debug && fs.Name == "" {
			return errors.New("created a null file")
		}
		de.dm.Add(fs)
	}
	if Debug && fs.Name == "" {
		return errors.New("created a null file")
	}

	if !forceUpdate && (fs.Checksum != "") {
		return nil
	}
	cks, err := CalcMd5File(de.dir, file)
	if err != nil {
		return err
	}
	if fs.Checksum == cks {
		return nil
	}
	log.Println("Recalculation of ", file, "found a changed checksum")
	fs.Checksum = cks
	fs.ArchivedAt = []string{}
	if Debug && fs.Name == "" {
		return errors.New("about to add a null file")
	}
	de.dm.Add(fs)

	return nil
}

// DeleteMissingFiles Delete any file entries that are in the dm,
// but not on the disk
// FIXME, this should be a method on dm
// FIXME write a test for this
func (de DirectoryEntry) DeleteMissingFiles() error {
	// FIXME this would be more efficient to mark the fs
	// as part of our visit.
	// The we can just delete them then
	fc := func(fileName string, fs FileStruct) (FileStruct, error) {
		fp := filepath.Join(de.dir, fileName)
		_, err := os.Stat(fp)
		if errors.Is(err, os.ErrNotExist) {
			return fs, errDeleteThisEntry
		}
		return fs, errIgnoreThisMutate
	}
	return de.dm.rangeMutate(fc)
}
