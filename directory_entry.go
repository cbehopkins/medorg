package medorg

import (
	"fmt"
	"io/fs"
	"log"
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
	workItems  chan WorkItem
	closeChan  chan struct{}
	fileWorker DirectoryVisitorFunc
	dir        string
	errorChan  chan error
	dm         DirectoryMap
}

func NewDirectoryEntry(path string, fw DirectoryVisitorFunc) DirectoryEntry {
	itm := new(DirectoryEntry)
	itm.dir = path
	itm.workItems = make(chan WorkItem)
	itm.closeChan = make(chan struct{})
	itm.errorChan = make(chan error)
	itm.fileWorker = fw
	// TBD, can we go this somehow? Do we even need to if we read it in quick enough?
	itm.dm = DirectoryMapFromDir(path)
	go itm.worker()
	return *itm
}
func (de DirectoryEntry) Close(directory string) <-chan error {
	fmt.Println("Closing entry:", de.dir)
	close(de.closeChan)
	return de.errorChan
}
func (de DirectoryEntry) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
	de.workItems <- WorkItem{dir, file, d, callback}
}

func (de DirectoryEntry) worker() {
	var activeFiles sync.WaitGroup
	defer close(de.errorChan)

	// allow file paths to be sent to us for processing
	for {
		select {
		case wi := <-de.workItems:
			activeFiles.Add(1)
			go func(dir, file string, d fs.DirEntry) {
				if de.fileWorker != nil {
					err := de.fileWorker(de, dir, file, d)
					if err != nil {
						de.errorChan <- err
					}
				}
				activeFiles.Done()
				wi.callback()
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
	// FIXME sort out proper error handling here
	de.dm.WriteDirectory(de.dir)
	return nil
}

// UpdateValues in the DirectoryEntry to those found on the fs
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
	if !fs.Changed(info) {
		return nil
	}
	fs.Mtime = info.ModTime().Unix()
	fs.Size = info.Size()
	fs.Checksum = ""
	de.dm.Add(fs)
	return nil
}
func (de DirectoryEntry) SetFs(fs FileStruct) {
	de.dm.Add(fs)
}

// UpdateChecksum will recalc the checksum of an entry
func (de DirectoryEntry) UpdateChecksum(file string, forceUpdate bool) error {
	if file == "" {
		log.Fatal("Updating a checksum on a null file")
	}

	fs, ok := de.dm.Get(file)
	if !ok {

		fsp, err := NewFileStruct(de.dir, file)
		if err != nil {
			return nil
		}
		fs = *fsp
		if fs.Name == "" {
			log.Fatal("Created a null file")
		}
		de.dm.Add(fs)
	}
	if fs.Name == "" {
		log.Fatal("We now have a null file")
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
	if fs.Checksum != "" {
		fmt.Println("Recalculation of ", file, "found a changed checksum")
	}
	fs.Checksum = cks

	if fs.Name == "" {
		log.Fatal("about to add a null file")
	}
	de.dm.Add(fs)

	return nil
}
