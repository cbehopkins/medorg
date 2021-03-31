package medorg

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
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

func TestDuplicateArchivedAtPopulation(t *testing.T) {
	// As per TestDuplicateDetect, but have they had the
	// ArchivedAt tag populated appropriatly
	dirs, err := createTestBackupDirectories(20, 10)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()

	backupLabelName := "tstBackup"
	t.Log("Created Test Directories:", dirs)
	scanBackupDirectories(dirs[1], dirs[0], backupLabelName)

	expectedDuplicates := 10
	archiveWalkFunc := func(directory, fn string, fs FileStruct, dm *DirectoryMap) bool {
		if fs.HasTag(backupLabelName) {
			expectedDuplicates--
		}
		return false
	}

	NewTreeWalker().WalkTree(dirs[0], archiveWalkFunc, nil)
	if expectedDuplicates != 0 {
		t.Error("Expected 0 duplicates left, got:", expectedDuplicates)
	}
}

// TBD add a test where the source already contains references to stuff in the destination

func TestBackupExtract(t *testing.T) {
	// Following on from TestDuplicateArchivedAtPopulation
	// We have correctly detected the duplicates and populated the
	// tags with this information.
	// Knowing this, we wnat to make sure when we scan though that tagged dir
	// we select the correct files to back up.
	srcFiles := 20
	numberBackedUp := 10
	dirs, err := createTestBackupDirectories(srcFiles, numberBackedUp)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()

	backupLabelName := "tstBackup0"
	altBackupLabelName := "tstBackup1"
	t.Log("Created Test Directories:", dirs)

	scanBackupDirectories(dirs[1], dirs[0], backupLabelName)

	// Now hack it about so that we pretend  n of the files
	// are additionally backed up to an alternate location
	// This should not change the number sent, but
	// should change the order things come out ijn
	numDuplicates := 1
	numExtra := 1
	extraMap := make(map[fpath]struct{})

	directoryWalker := func(dir, fn string, fs FileStruct) (FileStruct, bool) {
		if fs.HasTag(backupLabelName) {
			if numDuplicates > 0 {
				numDuplicates--
				fs.AddTag(altBackupLabelName)
				t.Log("Pretending", fn, "has additionally been backed up to alt location")
				return fs, true
			}
		} else {
			if numExtra > 0 {
				numExtra--
				fs.AddTag(altBackupLabelName)
				extraMap[fs.Path()] = struct{}{}
				t.Log("Pretending", fn, "has been backed up to alternate location")
				return fs, true
			}
		}
		return fs, false
	}

	tu := NewTreeUpdate(1, 1, 1)
	tu.UpdateDirectory(dirs[0], directoryWalker)

	copyFiles := extractCopyFiles(dirs[0], backupLabelName)

	cnt := 0
	expectedFilesToBackup := srcFiles - numberBackedUp
	primaryFileCount := expectedFilesToBackup - len(extraMap)
	for file := range copyFiles {
		t.Log("Received a file:", file)
		cnt++

		_, ok := extraMap[file]
		if ok {
			if primaryFileCount > 0 {
				t.Error("Got a file that is backed up elsewhere while we are still expecting primary files")
			}
			delete(extraMap, file)
		} else {
			if primaryFileCount > 0 {
				primaryFileCount--
			} else {
				t.Error("Extra primary file", file)
			}
		}

	}
	if cnt != expectedFilesToBackup {
		t.Error("Expected ", expectedFilesToBackup, " found:", cnt)
	}
}
