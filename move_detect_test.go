package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func createTestDirectories(root string, cnt int) ([]string, error) {
	directoriesCreated := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		name := filepath.Join(root, RandStringBytesMaskImprSrcSB(8))
		err := os.Mkdir(name, 0755)
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
func createTestMoveDetectDirectories(numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles int) (string, error) {
	dir, err := ioutil.TempDir("", "tstDir")
	if err != nil {
		return "", err
	}
	return dir, makeTestFilesAndDirectories(dir, numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles)
}

// We want a function to select n random files and m random directories
// The simplest way to do this would be to form a list of each
// shuffle the list, and then select n & m from them
func gatherFilesAndDirectories(root string) (files, directories []string) {
	walker := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		_, file := filepath.Split(path)
		if file == Md5FileName {
			return nil
		}
		if d.IsDir() {
			directories = append(directories, path)
			return nil
		}
		files = append(files, path)
		return nil
	}
	filepath.WalkDir(root, walker)
	return
}

var errMissingChecksum = errors.New("missing checksum")

func checkChecksums(de DirectoryEntry, directory, fn string, d fs.DirEntry) error {
	if fn == Md5FileName {
		return nil
	}
	_, ok := de.dm.Get(fn)
	if !ok {
		return fmt.Errorf("%w::%s", errMissingChecksum, fn)
	}
	return nil
}
func checkTestDirectoryChecksums(dir string) error {
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(dir)
			return dm, err
		}
		return NewDirectoryEntry(dir, checkChecksums, mkFk), nil
	}
	errChan := NewDirTracker(dir, makerFunc)
	for err := range errChan {
		for range errChan {
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func moveNfiles(cnt int, files, directories []string) error {
	directoryPointer := 0
	incrementDirectory := func() {
		directoryPointer++
		if directoryPointer > len(directories) {
			directoryPointer = 0
		}
	}
	seekCandidateDir := func(file string) bool {
		selectedDirectory := directories[directoryPointer]

		dir := filepath.Dir(file)
		srcDir, _ := filepath.Abs(dir)
		dstDir, _ := filepath.Abs(selectedDirectory)
		if srcDir == dstDir {
			incrementDirectory()
			return true
		}
		return false
	}
	for i := 0; i < cnt; i++ {
		selectedFile := files[i]
		for seekCandidateDir(selectedFile) {
		}
		selectedDirectory := directories[directoryPointer]
		MoveFile(Fpath(selectedFile), NewFpath(selectedDirectory, filepath.Base(selectedFile)))
		incrementDirectory()
	}
	return nil
}

func TestMoveDetect(t *testing.T) {
	type testSet struct {
		cfg   []int
		moveN int
	}
	testSet0 := []testSet{
		{cfg: []int{1, 0, 1}, moveN: 1},
		{cfg: []int{1, 1, 1}, moveN: 1},
		{cfg: []int{2, 0, 1}, moveN: 1},
		{cfg: []int{10, 1, 1}, moveN: 2},
		{cfg: []int{3, 3, 4}, moveN: 2},
		{cfg: []int{3, 3, 4}, moveN: 4},
		// {cfg: []int{4, 2, 8}, moveN: 16},
		// {cfg: []int{6, 4, 2}, moveN: 36},
		{cfg: []int{10, 2, 1}, moveN: 2},
		// {cfg: []int{100, 0, 1}, moveN: 2},
		// {cfg: []int{100, 1, 1}, moveN: 2},
		// {cfg: []int{1000, 0, 1}, moveN: 2},
		// {cfg: []int{10000, 0, 1}, moveN: 2},
	}

	for _, tst := range testSet0 {
		ts, moveN := tst.cfg, tst.moveN
		testName := fmt.Sprintln("Move Detect", moveN, "cfg", ts)

		t.Run(testName, func(t *testing.T) {
			movableCnt := (ts[0] * (ts[1] + 1) * ts[2])
			if moveN > movableCnt {
				t.Error("Invalid test", moveN, ">", movableCnt, ts)
			}
			movable := (ts[0] * (ts[1] + 1)) > 1
			root, err := createTestMoveDetectDirectories(ts[0], ts[1], ts[2])
			if err != nil {
				t.Error("Error creating test directories", err)
			}
			defer os.RemoveAll(root)
			files, directories := gatherFilesAndDirectories(root)
			err = recalcTestDirectory(root)
			if err != nil {
				t.Error("Error calculating initial checksums for directories", err)
			}
			err = checkTestDirectoryChecksums(root)
			if err != nil {
				t.Error("Error checking checksums for directories", err)
			}
			// t.Log("Created Test setup:", files, directories)
			_ = recalcTestDirectory(root)
			rand.Shuffle(len(files), func(i, j int) {
				files[i], files[j] = files[j], files[i]
			})
			rand.Shuffle(len(directories), func(i, j int) {
				directories[i], directories[j] = directories[j], directories[i]
			})
			t.Log("Now shuffled", root)

			// Move some files around
			err = moveNfiles(moveN, files, directories)
			if err != nil {
				t.Error("Error moving files", err)
			}
			err = checkTestDirectoryChecksums(root)
			if !errors.Is(err, errMissingChecksum) {
				if movable {
					t.Error("Error checking checksums for directories", err)
				}
			}
			err = NewMoveDetect().RunMoveDetect([]string{root})
			if err != nil {
				t.Error("move detect problem", err)
			}
			err = checkTestDirectoryChecksums(root)
			if err != nil {
				t.Error("Error checking checksums for moved directories", err)
			}
		})
	}
}
