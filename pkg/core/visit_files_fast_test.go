package core

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// fakeChecksumForTest creates a DirectoryMap entry with a fake checksum instead of calculating MD5.
// This is much faster for tests that only verify file visiting logic, not checksum correctness.
func fakeChecksumForTest(dm DirectoryMap, dir, fn string, d fs.DirEntry) error {
	if fn == Md5FileName {
		return nil
	}

	fileInfo, err := d.Info()
	if err != nil {
		return err
	}
	fileStruct, err := NewFileStruct(dir, fn)
	if err != nil {
		return err
	}

	// Use a fake checksum based on the file path instead of calculating MD5
	fileStruct.Checksum = fmt.Sprintf("fake-checksum-%s", filepath.Join(dir, fn))
	fileStruct.Size = fileInfo.Size()
	dm.Add(fileStruct)
	return nil
}

// fastRecalcTestDirectory creates a DirectoryMap for test directory with fake checksums.
// Much faster than recalcTestDirectory since it skips MD5 calculation.
func fastRecalcTestDirectory(dir string) error {
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			if err != nil {
				return nil, err
			}
			dm.VisitFunc = fakeChecksumForTest
			return dm, err
		}
		return NewDirectoryEntry(dir, mkFk)
	}
	for err := range NewDirTracker(false, dir, makerFunc).ErrChan() {
		return err
	}
	return nil
}

// TestVisitFilesInDirectoryFast is identical to TestVisitFilesInDirectory but uses fake checksums for speed
func TestVisitFilesInDirectoryFast(t *testing.T) {
	testSet0 := []struct {
		cfg []int
	}{
		{cfg: []int{1, 0, 1}},
		{cfg: []int{10, 1, 1}},
		{cfg: []int{4, 2, 8}},
		{cfg: []int{10, 2, 1}},
		{cfg: []int{100, 0, 1}},
	}

	for _, tst := range testSet0 {
		ts := tst.cfg
		testName := fmt.Sprintln("TestVisitFilesInDirectoryFast", ts)

		t.Run(testName, func(t *testing.T) {
			root, err := createTestMoveDetectDirectories(ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}
			defer os.RemoveAll(root)
			err = fastRecalcTestDirectory(root)
			if err != nil {
				t.Error("Error calculating initial checksums for directories", err)
			}
			var visitedFiles uint32
			expectedVisitCount := moveDetectDirCreationCount(ts[0], ts[1], ts[2])

			registerFunc := func(dt *DirTracker) {}
			someVisitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error {
				atomic.AddUint32(&visitedFiles, 1)
				return nil
			}
			errChan := VisitFilesInDirectories([]string{root}, registerFunc, someVisitFunc)
			for err := range errChan {
				t.Error("Rxd", err)
			}

			act := atomic.LoadUint32(&visitedFiles)
			if expectedVisitCount != int(act) {
				t.Error("error:", expectedVisitCount, act)
			}
		})
	}
}
