package core

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func recalcForTest(dm DirectoryMap, directory, fn string, d fs.DirEntry) error {
	if fn == Md5FileName {
		return nil
	}
	err := dm.UpdateValues(directory, d)
	if err != nil {
		return err
	}
	err = dm.UpdateChecksum(directory, fn, false)
	return err
}

func recalcTestDirectory(dir string) error {
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			dm.VisitFunc = recalcForTest
			return dm, err
		}
		return NewDirectoryEntry(dir, mkFk)
	}
	for err := range NewDirTracker(false, dir, makerFunc).ErrChan() {
		return fmt.Errorf("Error received on closing:%w", err)
	}
	return nil
}

func createTestDirectories(root string, cnt int) ([]string, error) {
	directoriesCreated := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		name := filepath.Join(root, RandStringBytesMaskImprSrcSB(8))
		err := os.Mkdir(name, 0o755)
		if err != nil {
			return []string{}, err
		}
		directoriesCreated[i] = name
	}
	return directoriesCreated, nil
}

func createTestFiles(directory string, numberOfFiles int) {
	for i := 0; i < numberOfFiles; i++ {
		_ = makeFile(directory)
	}
}

func createTestMoveDetectDirectories(numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles int) (string, error) {
	dir, err := os.MkdirTemp("", "tstDir")
	if err != nil {
		return "", err
	}
	return dir, makeTestFilesAndDirectories(dir, numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles)
}

func makeTestFilesAndDirectories(directory string, numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles int) error {
	directoriesCreated, err := createTestDirectories(directory, numberOfDirectoriesWide)
	if err != nil {
		return err
	}

	for _, v := range directoriesCreated {
		createTestFiles(v, numberOfFiles)
		if numberOfDirectoriesDeep > 0 {
			err := makeTestFilesAndDirectories(v, numberOfDirectoriesWide, numberOfDirectoriesDeep-1, numberOfFiles)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func moveDetectDirCreationCount(numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles int) int {
	runningCnt := 0
	for i := 0; i < numberOfDirectoriesWide; i++ {
		runningCnt += numberOfFiles
		if numberOfDirectoriesDeep > 0 {
			runningCnt += moveDetectDirCreationCount(numberOfDirectoriesWide, numberOfDirectoriesDeep-1, numberOfFiles)
		}
	}
	return runningCnt
}

func TestVisitFilesInDirectory(t *testing.T) {
	type testSet struct {
		cfg []int
	}
	testSet0 := []testSet{
		{cfg: []int{1, 0, 1}},
		{cfg: []int{1, 1, 1}},
		{cfg: []int{2, 0, 1}},
		{cfg: []int{10, 1, 1}},
		{cfg: []int{3, 3, 4}},
		{cfg: []int{4, 2, 8}},
		{cfg: []int{10, 2, 1}},
		// {cfg: []int{100, 0, 1}},
		// {cfg: []int{100, 1, 1}},
		// {cfg: []int{1000, 0, 1}},
		// {cfg: []int{10000, 0, 1}},
	}

	for _, tst := range testSet0 {
		ts := tst.cfg
		testName := fmt.Sprintln("TestVisitFilesInDirectory", ts)

		t.Run(testName, func(t *testing.T) {
			root, err := createTestMoveDetectDirectories(ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}
			defer os.RemoveAll(root)
			err = recalcTestDirectory(root)
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

func TestVisitFilesInDirectory1(t *testing.T) {
	type testSet struct {
		cfg []int
	}
	testSet0 := []testSet{
		{cfg: []int{1, 0, 1}},
		{cfg: []int{1, 1, 1}},
		{cfg: []int{2, 0, 1}},
		{cfg: []int{10, 1, 1}},
		{cfg: []int{3, 3, 4}},
		{cfg: []int{4, 2, 8}},
		{cfg: []int{10, 2, 1}},
		{cfg: []int{100, 0, 1}},
		// {cfg: []int{100, 1, 1}},
		// {cfg: []int{1000, 0, 1}},
		// {cfg: []int{10000, 0, 1}},
	}

	for _, tst := range testSet0 {
		ts := tst.cfg
		testName := fmt.Sprintln("TestVisitFilesInDirectory", ts)

		t.Run(testName, func(t *testing.T) {
			root, err := createTestMoveDetectDirectories(ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}
			defer os.RemoveAll(root)
			err = recalcTestDirectory(root)
			if err != nil {
				t.Error("Error calculating initial checksums for directories", err)
			}
			var visitedFiles uint32
			expectedVisitCount := moveDetectDirCreationCount(ts[0], ts[1], ts[2])

			registerFunc := func(dt *DirTracker) {
				log.Println("Registering  Dirtracker error handler start")
			}
			someVisitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error {
				log.Println("Visit 0", dir, fn)
				atomic.AddUint32(&visitedFiles, 1)
				return nil
			}
			dta := AutoVisitFilesInDirectories([]string{root}, someVisitFunc)

			for err := range errHandler(dta, registerFunc) {
				t.Error("Rxd", err)
			}

			act := atomic.LoadUint32(&visitedFiles)
			if expectedVisitCount != int(act) {
				t.Error("error:", expectedVisitCount, act)
			}

			var reVisitCount uint32
			fileVisitFunc := func(dm DirectoryEntryInterface, dir, fn string, fileStruct FileStruct) error {
				log.Println("Visit 1", dir, fn)
				atomic.AddUint32(&reVisitCount, 1)
				return nil
			}
			directoryVisitor := func(dt *DirTracker) {}
			for _, dt := range dta {
				dt.RevisitAll(root, directoryVisitor, fileVisitFunc, nil)
			}

			act = atomic.LoadUint32(&reVisitCount)
			if expectedVisitCount != int(act) {
				t.Error("error:", expectedVisitCount, act)
			}
		})
	}
}
