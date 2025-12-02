package core

import (
	"io/fs"
	"log"
	"sync"
)

// VisitFilesWithInterface: Interface-based visitor for decoupled code
// Uses FileMetadata and DirectoryStorage interfaces instead of concrete types
func VisitFilesWithInterface(
	directories []string,
	registerFunc func(dt *DirTracker),
	visitor ExtendedDirectoryVisitor,
) <-chan error {
	// Wrap the interface visitor to work with the legacy implementation
	legacyVisitor := func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error {
		return visitor.Visit(&dm, dir, fn, d, &fileStruct, fileInfo)
	}
	return VisitFilesInDirectories(directories, registerFunc, legacyVisitor)
}

// VisitFilesInDirectories: You should default to using this utility function where you can
// It's probably what you want!
// You can supply a visitor and get the fileStruct associated with the file in question
// Any changes you make to that will be reflected on disk
// Note it only visits files that already have an entry (This might need to be fixed?)
func VisitFilesInDirectories(
	directories []string,
	registerFunc func(dt *DirTracker),
	someVisitFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error,
) <-chan error {
	dts := AutoVisitFilesInDirectories(directories, someVisitFunc)
	return errHandler(dts, registerFunc)
}

func errHandler(
	dts []*DirTracker,
	registerFunc func(dt *DirTracker),
) <-chan error {
	if registerFunc == nil {
		registerFunc = func(dt *DirTracker) {}
	}
	errChan := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(dts))
	for _, ndt := range dts {
		registerFunc(ndt)
		go func(ndt *DirTracker) {
			for err := range ndt.ErrChan() {
				log.Println("Error received", err)
				if err != nil {
					errChan <- err
				}
			}
			wg.Done()
		}(ndt)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	return errChan
}

func AutoVisitFilesInDirectories(
	directories []string,
	someVisitFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error,
) []*DirTracker {
	if someVisitFunc == nil {
		someVisitFunc = func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error {
			return nil
		}
	}
	visitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == Md5FileName {
			return nil
		}
		fileStruct, ok := dm.Get(fn)
		if !ok {
			var err error
			// FIXME we could use the fileInfo from below if we had the option
			fileStruct, err = NewFileStruct(dir, fn)
			if err != nil {
				return err
			}
			dm.Add(fileStruct)
		}
		fileInfo, err := d.Info()
		if err != nil {
			return err
		}

		return someVisitFunc(dm, dir, fn, d, fileStruct, fileInfo)
	}

	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.VisitFunc = visitFunc
			return dm, err
		}
		return NewDirectoryEntry(dir, mkFk)
	}
	retArray := make([]*DirTracker, len(directories))
	for i, targetDir := range directories {
		retArray[i] = NewDirTracker(true, targetDir, makerFunc)
	}
	return retArray
}
