package consumers

import (
	"errors"
	"io/fs"
	"os"
	"sync"

	"github.com/cbehopkins/medorg/pkg/core"
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
	dupeMap map[moveKey]core.FileStruct
}

// runMoveDetectFindDeleted will visit all directories looking for files which have been deleted
// Collects deleted file properties for matching in the second pass
func (mvd *moveDetect) runMoveDetectFindDeleted(directory string) error {
	visitFunc := func(dm core.DirectoryMap, path core.Fpath, d fs.DirEntry, fileStruct core.FileStruct, fileInfo os.FileInfo) error {
		if path.Is(core.Md5FileName) {
			return nil
		}
		// Check if the file still exists on disk
		_, err := os.Stat(path.String())
		if errors.Is(err, os.ErrNotExist) {
			// File doesn't exist, add to move detect map for later matching
			mvd.add(fileStruct)
		}
		return nil
	}

	errChan := core.VisitFilesInDirectories([]string{directory}, nil, visitFunc)
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

// runMoveDetectFindNew will visit all directories looking for new files
// If they match deleted files (by size and name), adds them to the map
func (mvd *moveDetect) runMoveDetectFindNew(directory string) error {
	visitFunc := func(dm core.DirectoryMap, path core.Fpath, d fs.DirEntry, fileStruct core.FileStruct, fileInfo os.FileInfo) error {
		if path.Is(core.Md5FileName) {
			return nil
		}

		// Check if this file matches any deleted file by size and name
		v, err := mvd.query(fileInfo)
		if err == errMvdQueryFailed {
			// No match found, skip
			return nil
		}
		if err != nil {
			return err
		}

		// Found a match! Update the file with the deleted file's metadata
		v.SetDirectory(path.Dir())
		dm.Add(v)
		mvd.delete(v)

		// Update the file values in the map
		return dm.UpdateValues(path.Dir(), d)
	}

	errChan := core.VisitFilesInDirectories([]string{directory}, nil, visitFunc)
	for err := range errChan {
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

	// Run first pass in parallel - find deleted files
	errChan := make(chan error, len(dirs))
	var wg sync.WaitGroup
	wg.Add(len(dirs))

	for _, dir := range dirs {
		go func(d string) {
			defer wg.Done()
			if err := mvd.runMoveDetectFindDeleted(d); err != nil {
				errChan <- err
			}
		}(dir)
	}

	wg.Wait()
	close(errChan)

	// Check for errors from first pass
	for err := range errChan {
		return err
	}

	// Run second pass - find new files with matching properties
	for _, dir := range dirs {
		err := mvd.runMoveDetectFindNew(dir)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mvd *moveDetect) add(fileStruct core.FileStruct) {
	mvd.Lock()
	if mvd.dupeMap == nil {
		mvd.dupeMap = make(map[moveKey]core.FileStruct)
	}
	mvd.dupeMap[moveKey{fileStruct.Size, string(fileStruct.Name)}] = fileStruct
	mvd.Unlock()
}

func (mvd *moveDetect) delete(fileStruct core.FileStruct) {
	if mvd.dupeMap == nil {
		return
	}
	mvd.Lock()
	delete(mvd.dupeMap, moveKey{fileStruct.Size, string(fileStruct.Name)})
	mvd.Unlock()
}

// query if the file struct (equivalent) is in the move detect array
func (mvd *moveDetect) query(fi os.FileInfo) (core.FileStruct, error) {
	mvd.RLock()
	defer mvd.RUnlock()
	if mvd.dupeMap == nil {
		return core.FileStruct{}, errMvdQueryFailed
	}
	key := moveKey{fi.Size(), fi.Name()}
	v, ok := mvd.dupeMap[key]
	if !ok {
		return core.FileStruct{}, errMvdQueryFailed
	}
	return v, nil
}
