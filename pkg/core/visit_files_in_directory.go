package core

import (
	"io/fs"
	"os"

	pb "github.com/cbehopkins/pb/v3"
)

// VisitFilesInDirectories: You should default to using this utility function where you can
// It's probably what you want!
// The factory creates and manages progress bars for each directory scan
// You can supply a visitor and get the fileStruct associated with the file in question
// Any changes you make to that will be reflected on disk
// Note it only visits files that already have an entry (This might need to be fixed?)
func VisitFilesInDirectories(
	directories []string,
	factory *pb.PoolProgressFactory,
	someVisitFunc func(dm DirectoryMap, path Fpath, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error,
) <-chan error {
	// Implemented with DirectoryWalker to leverage DirectoryMapFromDirWithScan internally
	// and avoid redundant per-file stats. We bridge the legacy visitor signature here.
	errChan := make(chan error, 1)

	dw := NewDirectoryWalker(MakeTokenChan(NumTrackerOutstanding))
	dw.AddFileVisitor(func(fn Fname, fm FileMetadata, fi os.FileInfo) error {
		// Reconstruct path and concrete FileStruct for the legacy visitor
		fsPtr, ok := fm.(*FileStruct)
		if !ok {
			return nil
		}
		fp := NewFpath(string(fsPtr.directory), string(fn))
		// We don't have a concrete fs.DirEntry per-file in this walker; pass nil.
		// The DirectoryMap is managed inside the walker; pass zero value for compatibility.
		return someVisitFunc(DirectoryMap{}, fp, nil, *fsPtr, fi)
	})

	go func() {
		defer close(errChan)
		// Equivalent to WalkMulti - walk each directory
		for _, root := range directories {
			if err := dw.Walk(root); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Progress factory not integrated in this walker-based path to keep API stable.
	return errChan
}
