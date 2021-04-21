package medorg

import (
	"errors"
	"io/fs"
	"os"
	"sync"
)

var errMvdQueryFailed = errors.New("query failed")

type moveKey struct {
	size int64
	name string
}
type MoveDetect struct {
	sync.RWMutex
	dupeMap map[moveKey]FileStruct
}

func NewMoveDetect() *MoveDetect {
	var itm MoveDetect
	itm.dupeMap = make(map[moveKey]FileStruct)
	return &itm
}

// runMoveDetectFindDeleted will run through the directory
// looking for any files which have been deleted
// And move the FileStruct from the dm into a map
func (mvd *MoveDetect) runMoveDetectFindDeleted(directory string) error {
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
			return dm, err
		}
		md := NewDirectoryEntry(dir, visitFunc, mkFk)
		// This need some rework in our interface so that
		// makerFunc can retuen an error to the NewDirTracker that is creating it
		return md, md.dm.rangeMutate(fc)
	}
	for err := range NewDirTracker(directory, makerFunc) {
		if err != nil {
			return err
		}
	}
	return nil
}

// runMoveDetectFindNew will run through the directory
// looking for any new files and if they exist in the map
// then populate the entry withou a calculation
func (mvd *MoveDetect) runMoveDetectFindNew(directory string) error {
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
		// BUM
		v.directory = dir
		dm.Add(v)
		mvd.delete(v)
		return dm.UpdateValues(dir, d)
	}
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			return dm, err
		}
		return NewDirectoryEntry(dir, visitFunc, mkFk), nil
	}
	errChan := NewDirTracker(directory, makerFunc)
	for err := range errChan {
		for range errChan {
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (mvd *MoveDetect) RunMoveDetect(dirs []string) error {
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
func (mvd *MoveDetect) add(fileStruct FileStruct) {
	mvd.Lock()
	mvd.dupeMap[moveKey{fileStruct.Size, fileStruct.Name}] = fileStruct
	mvd.Unlock()
}
func (mvd *MoveDetect) delete(fileStruct FileStruct) {
	mvd.Lock()
	delete(mvd.dupeMap, moveKey{fileStruct.Size, fileStruct.Name})
	mvd.Unlock()
}

// query if the file struct (equivalent) is in the move detect array
func (mvd *MoveDetect) query(d fs.DirEntry) (FileStruct, error) {
	info, err := d.Info()
	if err != nil {
		return FileStruct{}, err
	}
	mvd.RLock()
	defer mvd.RUnlock()
	key := moveKey{info.Size(), info.Name()}
	v, ok := mvd.dupeMap[key]
	if !ok {
		return FileStruct{}, errMvdQueryFailed
	}
	return v, nil
}
