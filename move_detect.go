package medorg

import (
	"errors"
	"io/fs"
	"os"
	"sync"
)

var errMvdQueryFailed = errors.New("query failed")

// moveKey is the key for the move detect map
// We're looking for a file with the same name and size
// just in a different location
// This saves us re-calculating the checksum
type moveKey struct {
	size int64
	name string
}

type moveDetect struct {
	sync.RWMutex
	dupeMap map[moveKey]FileStruct
}

// runMoveDetectFindDeleted will run through the directory
// looking for any files which have been deleted
// And move the FileStruct from the dm into a map
func (mvd *moveDetect) runMoveDetectFindDeleted(directory string) error {
	visitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		return nil
	}
	fc := func(fn string, fileStruct FileStruct) (FileStruct, error) {
		_, err := os.Stat(string(fileStruct.Path()))
		if !errors.Is(err, os.ErrNotExist) {
			return fileStruct, errIgnoreThisMutate
		}
		// The file does not exist on the disk, so
		// add it to our list of files
		mvd.add(fileStruct)
		return fileStruct, errDeleteThisEntry
	}
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.VisitFunc = visitFunc
			if err != nil {
				return dm, err
			}
			return dm, dm.rangeMutate(fc)
		}
		return NewDirectoryEntry(dir, mkFk)
	}
	for err := range NewDirTracker(directory, makerFunc).ErrChan() {
		if err != nil {
			return err
		}
	}
	return nil
}

// runMoveDetectFindNew will run through the directory
// looking for any new files and if they exist in the map
// then populate the entry withou a calculation
func (mvd *moveDetect) runMoveDetectFindNew(directory string) error {
	visitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == Md5FileName {
			return nil
		}
		v, err := mvd.query(d)
		if err == errMvdQueryFailed {
			return nil
		}
		if err != nil {
			return err
		}
		v.directory = dir
		dm.Add(v)
		mvd.delete(v)
		return dm.UpdateValues(dir, d)
	}
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.VisitFunc = visitFunc
			return dm, err
		}
		return NewDirectoryEntry(dir, mkFk)
	}
	errChan := NewDirTracker(directory, makerFunc).ErrChan()
	for err := range errChan {
		for range errChan {
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// RunMoveDetect the move detect on specified directories
// First get the properties for any files that have been deleted
// Then a second pass to see if new files with matching
// properties have been added
func RunMoveDetect(dirs []string) error {
	var mvd moveDetect
	for _, dir := range dirs {
		// FIXME we should be able to run this in parallel
		err := mvd.runMoveDetectFindDeleted(dir)
		if err != nil {
			return err
		}
	}
	for _, dir := range dirs {
		err := mvd.runMoveDetectFindNew(dir)
		if err != nil {
			return err
		}
	}
	return nil
}
func (mvd *moveDetect) add(fileStruct FileStruct) {
	mvd.Lock()
	if mvd.dupeMap == nil {
		mvd.dupeMap = make(map[moveKey]FileStruct)
	}
	mvd.dupeMap[moveKey{fileStruct.Size, fileStruct.Name}] = fileStruct
	mvd.Unlock()
}
func (mvd *moveDetect) delete(fileStruct FileStruct) {
	if mvd.dupeMap == nil {
		return
	}
	mvd.Lock()
	delete(mvd.dupeMap, moveKey{fileStruct.Size, fileStruct.Name})
	mvd.Unlock()
}

// query if the file struct (equivalent) is in the move detect array
func (mvd *moveDetect) query(d fs.DirEntry) (FileStruct, error) {
	info, err := d.Info()
	if err != nil {
		return FileStruct{}, err
	}
	mvd.RLock()
	defer mvd.RUnlock()
	if mvd.dupeMap == nil {
		return FileStruct{}, errMvdQueryFailed
	}
	key := moveKey{info.Size(), info.Name()}
	v, ok := mvd.dupeMap[key]
	if !ok {
		return FileStruct{}, errMvdQueryFailed
	}
	return v, nil
}
