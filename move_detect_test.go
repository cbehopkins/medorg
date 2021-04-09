package medorg

import (
	"errors"
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
		return errMissingChecksum
	}
	return nil
}
func checkTestDirectoryChecksums(dir string) error {
	makerFunc := func(dir string) DirectoryTrackerInterface {
		return NewDirectoryEntry(dir, checkChecksums)
	}
	errChan := NewDirTracker(dir, makerFunc)
	for err := range errChan {
		go func() {
			for range errChan {
			}
		}()
		return err
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
	root, err := createTestMoveDetectDirectories(1, 2, 1)
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
	t.Log("Created Test setup:", files, directories)
	_ = recalcTestDirectory(root)
	rand.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
	rand.Shuffle(len(directories), func(i, j int) {
		directories[i], directories[j] = directories[j], directories[i]
	})
	t.Log("Now shuffled")

	// Move some files around
	err = moveNfiles(2, files, directories)
	if err != nil {
		t.Error("Error moving files", err)
	}
	err = checkTestDirectoryChecksums(root)
	if err != errMissingChecksum {
		t.Error("Error checking checksums for directories", err)
	}
}
