package medorg

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createTestBackupDirectories(numberOfFiles, numberOfDuplicates int) ([]string, error) {
	if numberOfDuplicates > numberOfFiles {
		return nil, errors.New("You asked for more duplicates than files")
	}
	directoriesCreated := make([]string, 2)
	for i := 0; i < 2; i++ {
		dir, err := ioutil.TempDir("", "tstDir")
		if err != nil {
			return nil, err
		}
		directoriesCreated[i] = dir
	}

	filenames := make([]string, numberOfFiles)
	// Make a bunch of files in the src directory
	for i := 0; i < numberOfFiles; i++ {
		filenames[i] = makeFile(directoriesCreated[0])
	}
	randomSrc := rand.Perm(numberOfFiles)
	for i := 0; i < numberOfDuplicates; i++ {
		selectedFilename := filenames[randomSrc[i]]
		stem := filepath.Base(selectedFilename)
		dstFile := Fpath(directoriesCreated[1], stem)
		err := CopyFile(fpath(selectedFilename), dstFile)
		if err != nil {
			return nil, err
		}
	}
	return directoriesCreated, nil
}

// Test whether we can detect duplicates within the
func TestDuplicateDetect(t *testing.T) {
	dirs, err := createTestBackupDirectories(20, 10)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()
	t.Log("Created Test Directories:", dirs)
	tu := NewTreeUpdate(1, 1, 1)
	srcTm := make(backupDupeMap)
	dstTm := make(backupDupeMap)

	mfSrc := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		srcTm.add(fs)
		return fs, false
	}
	mfDst := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		dstTm.add(fs)
		return fs, false
	}

	// First we populate the src dir
	tu.UpdateDirectory(dirs[1], mfSrc)
	tu.Init() // FIXME - bit rubbish that this is needed
	tu.UpdateDirectory(dirs[0], mfDst)
	matchChan := srcTm.findDuplicates(dstTm)
	expectedDuplicates := 10
	for val := range matchChan {
		expectedDuplicates--

		t.Log(val)
	}
	if expectedDuplicates != 0 {
		t.Error("Expected 0 duplicates left, got:", expectedDuplicates)
	}
}

func TestDuplicatePopulation(t *testing.T) {
	dirs, err := createTestBackupDirectories(20, 10)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()
	t.Log("Created Test Directories:", dirs)
	tu := NewTreeUpdate(1, 1, 1)
	srcTm := make(backupDupeMap)
	dstTm := make(backupDupeMap)

	mfSrc := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		srcTm.add(fs)
		return fs, false
	}
	mfDst := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		key := backupKey{fs.Size, fs.Checksum}
		_, ok := srcTm[key]
		if ok {
			fs.ArchivedAt = append(fs.ArchivedAt, "tstBackup")
		}
		dstTm.add(fs)
		fs.Analysed = time.Now().Unix()

		return fs, true
	}
	expectedDuplicates := 10
	archiveWalkFunc := func(directory, fn string, fs FileStruct, dm *DirectoryMap) bool {
		for _, v := range fs.ArchivedAt {
			if v == "tstBackup" {
				expectedDuplicates--
			}
		}
		return false
	}
	// First we populate the src dir
	tu.UpdateDirectory(dirs[1], mfSrc)
	tu.Init() // FIXME - bit rubbish that this is needed
	tu.UpdateDirectory(dirs[0], mfDst)

	NewTreeWalker().WalkTree(dirs[0], archiveWalkFunc, nil)
	if expectedDuplicates != 0 {
		t.Error("Expected 0 duplicates left, got:", expectedDuplicates)
	}
}
