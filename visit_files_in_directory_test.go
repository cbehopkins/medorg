package medorg

import (
	"fmt"
	"io/fs"
	"os"
	"sync/atomic"
	"testing"
)

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
			dta := AutoVisitFilesInDirectories([]string{root}, someVisitFunc)

			for err := range errHandler(dta, registerFunc) {
				t.Error("Rxd", err)
			}

			act := atomic.LoadUint32(&visitedFiles)
			if expectedVisitCount != int(act) {
				t.Error("error:", expectedVisitCount, act)
			}

			// var reVisitCount uint32
			// someVisitFunc := func(dm DirectoryMap, dir, fn string, d fs.DirEntry, fileStruct FileStruct, fileInfo fs.FileInfo) error {
			// 	atomic.AddUint32(&reVisitCount, 1)
			// 	return nil
			// }
			// for _, dt := range dta {
			// 	dt.Revisit(nil, someVisitFunc)
			// }

			// act = atomic.LoadUint32(&reVisitCount)
			// if expectedVisitCount != int(act) {
			// 	t.Error("error:", expectedVisitCount, act)
			// }
		})
	}
}
