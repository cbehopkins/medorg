package core

import (
	"io/fs"
	"log"
	"sync"

	pb "github.com/cbehopkins/pb/v3"
)

// VisitFilesWithInterface: Interface-based visitor for decoupled code
// Uses FileMetadata and DirectoryStorage interfaces instead of concrete types
func VisitFilesWithInterface(
	directories []string,
	factory *pb.PoolProgressFactory,
	visitor ExtendedDirectoryVisitor,
) <-chan error {
	// Wrap the interface visitor to work with the legacy implementation
	legacyVisitor := func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error {
		return visitor.Visit(&dm, dir, fn, d, &fileStruct, fileInfo)
	}
	return VisitFilesInDirectories(directories, factory, legacyVisitor)
}

// VisitFilesInDirectories: You should default to using this utility function where you can
// It's probably what you want!
// The factory creates and manages progress bars for each directory scan
// You can supply a visitor and get the fileStruct associated with the file in question
// Any changes you make to that will be reflected on disk
// Note it only visits files that already have an entry (This might need to be fixed?)
func VisitFilesInDirectories(
	directories []string,
	factory *pb.PoolProgressFactory,
	someVisitFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error,
) <-chan error {
	dts := AutoVisitFilesInDirectories(directories, someVisitFunc)
	return errHandler(dts, factory)
}

func errHandler(
	dts []*DirTracker,
	factory *pb.PoolProgressFactory,
) <-chan error {
	errChan := make(chan error, len(dts)) // Buffer with capacity = number of senders
	var wg sync.WaitGroup
	wg.Add(len(dts))
	for _, ndt := range dts {
		if factory != nil {
			if err := factory.Register(ndt); err != nil {
				log.Printf("failed to register progress: %v", err)
			}
		}
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
		if factory != nil && factory.Wg != nil {
			factory.Wg.Wait()
		}
		close(errChan)
	}()
	return errChan
}

// AutoVisitFilesInDirectoriesWithTokens is like AutoVisitFilesInDirectories but accepts a shared token channel
// for global concurrency control across all directories
func AutoVisitFilesInDirectoriesWithTokens(
	directories []string,
	fileProcessTokens chan struct{},
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
			// Use the DirEntry to get file info instead of redundant os.Stat
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}
			fileStruct, err = fileStruct.FromStat(dir, fn, fileInfo)
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
			if err == nil {
				dm.SetVisitFunc(visitFunc)
			}
			return dm, err
		}
		de, err := NewDirectoryEntryWithTokens(dir, mkFk, fileProcessTokens)
		return de, err
	}
	retArray := make([]*DirTracker, len(directories))
	for i, targetDir := range directories {
		retArray[i] = NewDirTrackerWithTokens(true, targetDir, makerFunc)
	}
	return retArray
}

func AutoVisitFilesInDirectories(
	directories []string,
	someVisitFunc func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error,
) []*DirTracker {
	return AutoVisitFilesInDirectoriesWithTokens(directories, nil, someVisitFunc)
}
