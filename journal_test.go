package medorg

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

type directoryTestStuff struct {
	Name  string
	de    *DirectoryEntry
	Dirs  []directoryTestStuff
	Files []FileStruct
}

// populateDirectoryStuff create a test directory
// that has fileCnt files nested under a directory dirDepth deep
func populateDirectoryStuff(dirDepth, fileCnt int) directoryTestStuff {
	randDirectory := func(len int) string {
		paths := make([]string, len)
		for i := range paths {
			paths[i] = RandStringBytesMaskImprSrcSB(6)
		}
		return filepath.Join(paths...)
	}
	dts := directoryTestStuff{
		Name: randDirectory(dirDepth),
	}
	dts.Files = make([]FileStruct, fileCnt)
	for i := 0; i < fileCnt; i++ {
		dts.Files[i] = FileStruct{Name: RandStringBytesMaskImprSrcSB(5), Checksum: RandStringBytesMaskImprSrcSB(8)}
	}
	return dts
}
func directoryMapFromStuff(path string, dts *directoryTestStuff, deChan chan<- DirectoryEntry) error {

	mkFk := func(path string) (DirectoryEntryInterface, error) {
		dm := NewDirectoryMap(nil)
		for _, fs := range dts.Files {
			dm.mp[fs.Name] = fs
		}
		return *dm, nil
	}
	if dts.de == nil {
		de, err := NewDirectoryEntry(filepath.Join(path, dts.Name), mkFk)
		if err != nil {
			return err
		}
		dts.de = &de
	}
	deChan <- *dts.de
	for i := range dts.Dirs {
		err := directoryMapFromStuff(dts.Name, &dts.Dirs[i], deChan)
		if err != nil {
			return err
		}
	}
	return nil
}
func runDirectory(t *testing.T, dts *directoryTestStuff, visitFunc func(de DirectoryEntry) error) {
	deChan := make(chan DirectoryEntry)
	go func() {
		err := directoryMapFromStuff("", dts, deChan)
		if err != nil {

			t.Error(err)
		}
		close(deChan)
	}()
	for de := range deChan {
		err := visitFunc(de)
		if err != nil {
			t.Error(err)
		}
	}
}
func modifyFilesInJournal(changesToMake int, dts directoryTestStuff) error {
	dm, ok := dts.de.dm.(DirectoryMap)
	if !ok {
		return errors.New("Unable to cast spell")
	}
	changeArray := make([]string, changesToMake)
	for filename := range dm.mp {
		if changesToMake <= 0 {
			break
		}
		changesToMake--
		changeArray[changesToMake] = filename
	}
	// Modify the filename on anything that's in the change array
	for _, v := range changeArray {
		newFilename := v + "_mod"
		fs := dm.mp[v]
		fs.Name = newFilename
		dm.mp[newFilename] = fs
		delete(dm.mp, v)
	}
	return nil
}

func createInitialJournal(t *testing.T, initialDirectoryStructure *directoryTestStuff) Journal {
	journal := Journal{}
	visitFuncInitial0 := func(de DirectoryEntry) error {
		tmp := de.dm.(DirectoryMap)
		//dm := tst.(DirectoryEntryJournalableInterface)
		err := journal.AppendJournalFromDm(&tmp, de.dir)
		if err == errFileExistsInJournal {
			return fmt.Errorf("initial setup TestJournalDummyWalks %w,%s", err, de.dir)
		}
		return err
	}
	// Startoff with an initial directory and get a journal
	// that has all those in
	runDirectory(t, initialDirectoryStructure, visitFuncInitial0)
	return journal
}

// TestJournalDummyWalk does not write anything to disk
// Everything is done through a dummy structure
// But basically check that
func TestJournalDummyWalk(t *testing.T) {
	initialDirectoryStructure := populateDirectoryStuff(2, 5)
	initialDirectoryStructure.Dirs = []directoryTestStuff{
		// populateDirectoryStuff(1, 5),
		// populateDirectoryStuff(1, 5),
		// populateDirectoryStuff(1, 5),
	}
	t.Log("Working with:", initialDirectoryStructure)
	journal := createInitialJournal(t, &initialDirectoryStructure)
	t.Log(journal)

	visitFuncRevisit := func(de DirectoryEntry) error {
		tmp := de.dm.(DirectoryMap)
		err := journal.AppendJournalFromDm(&tmp, de.dir)
		if err != errFileExistsInJournal {
			t.Log("Got:", de.dm)
			return fmt.Errorf("issue on revisit %w,%s", err, de.dir)
		}
		return nil
	}
	// Here we are going for a walk over the the test data
	// Nothing exists on disk in this test, but journal should report
	// having seen this already
	runDirectory(t, &initialDirectoryStructure, visitFuncRevisit)

}

// Again no file access, but pretend we are
// adding some files
func TestJournalDummyAddFiles(t *testing.T) {
	initialDirectoryStructure := populateDirectoryStuff(2, 5)
	initialDirectoryStructure.Dirs = []directoryTestStuff{
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
	}
	numFilesToAdd := 3
	numDirsToAdd := 1
	t.Log("Working with:", initialDirectoryStructure)
	journal := createInitialJournal(t, &initialDirectoryStructure)
	t.Log(journal)

	// Now let's add a few files
	// This should be a new directory with 3 files
	visitFuncInitial1 := func(de DirectoryEntry) error {
		tmp := de.dm.(DirectoryMap)
		err := journal.AppendJournalFromDm(&tmp, de.dir)
		if err == errFileExistsInJournal {
			return fmt.Errorf("initial1 TestJournalBasicXml %w,%s", err, de.dir)
		}
		numDirsToAdd--
		return err
	}
	td1 := populateDirectoryStuff(numDirsToAdd, numFilesToAdd)
	runDirectory(t, &td1, visitFuncInitial1)
	if numDirsToAdd != 0 {
		t.Error("Strange number of directories", numDirsToAdd)
	}
}

func TestJournalDummyModifyFiles(t *testing.T) {
	initialDirectoryStructure := populateDirectoryStuff(2, 5)
	initialDirectoryStructure.Dirs = []directoryTestStuff{
		// populateDirectoryStuff(1, 5),
		// populateDirectoryStuff(1, 5),
		// populateDirectoryStuff(1, 5),
	}

	expectedAdditions := 1
	t.Log("Working with:", initialDirectoryStructure)
	journal := createInitialJournal(t, &initialDirectoryStructure)
	t.Log(journal)

	// Now what happens if we send the same directory names, but with different files in them
	// We should see that they are treated the same as new directories
	err := modifyFilesInJournal(expectedAdditions, initialDirectoryStructure)
	if err != nil {
		t.Error(err)
	}

	visitFuncAdd0 := func(de DirectoryEntry) error {
		tmp := de.dm.(DirectoryMap)
		err := journal.AppendJournalFromDm(&tmp, de.dir)
		if err == errFileExistsInJournal {
			return nil
		}
		if err == nil {
			expectedAdditions--
		}
		return err
	}
	runDirectory(t, &initialDirectoryStructure, visitFuncAdd0)
	if expectedAdditions != 0 {
		t.Error("Strange number of expectedAdditions", expectedAdditions)
	}
}

func deleteNDirectories(n int, t *testing.T, initialDirectoryStructure directoryTestStuff, journal Journal) {
	// FIXME
	if n != 1 {
		t.Error("Bang")
	}
	bob := initialDirectoryStructure.Dirs[0]
	deletedDirectory := directoryTestStuff{
		Name: filepath.Join(initialDirectoryStructure.de.dir, bob.Name),
	}

	// Now we have an entry to delete, submit that to the journal
	visitFuncDeleter := func(de DirectoryEntry) error {
		tmp := de.dm.(DirectoryMap)
		err := journal.AppendJournalFromDm(&tmp, de.dir)
		if err == errFileExistsInJournal {
			// All files should already exist
			return nil
		}
		if err == nil {
			return errors.New("all files should exist")
		}
		return err
	}
	runDirectory(t, &deletedDirectory, visitFuncDeleter)
}

// TestJournalDummyRmDir will pretend that we have gone and deleted
// one of the sub directories
func TestJournalDummyRmDir(t *testing.T) {
	initialDirectoryStructure := populateDirectoryStuff(2, 5)
	initialDirectoryStructure.Dirs = []directoryTestStuff{
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
	}

	t.Log("Working with:", initialDirectoryStructure)
	journal := createInitialJournal(t, &initialDirectoryStructure)
	t.Log(journal)
	expectedDeletions := 1

	deleteNDirectories(expectedDeletions, t, initialDirectoryStructure, journal)

	// Now run our original directory structure
	visitFuncCheck := func(de DirectoryEntry) error {
		tmp := de.dm.(DirectoryMap)
		err := journal.AppendJournalFromDm(&tmp, de.dir)
		if err == errFileExistsInJournal {
			return nil
		}
		if err == nil {
			// Any deleted directories should behave as if deleted
			expectedDeletions--
		}
		return err
	}
	runDirectory(t, &initialDirectoryStructure, visitFuncCheck)

	if expectedDeletions != 0 {
		t.Error("Strange number of expectedDeletions", expectedDeletions)
	}
}

// TestJournalDummyVisitDirs will test that the journal records what we expect
func TestJournalDummyVisitDirs(t *testing.T) {
	initialDirectoryStructure := populateDirectoryStuff(2, 5)
	initialDirectoryStructure.Dirs = []directoryTestStuff{
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
	}

	t.Log("Working with:", initialDirectoryStructure)
	journal := createInitialJournal(t, &initialDirectoryStructure)
	t.Log(journal)

	expectedDirectoryCount := 1 + len(initialDirectoryStructure.Dirs)
	visitor := func(de DirectoryEntryJournalableInterface, dir string) error {
		expectedDirectoryCount--
		if de.Len() != 5 {
			t.Error("Wrong File count for:", de)
		}
		return nil
	}
	journal.Range(visitor)
	if expectedDirectoryCount != 0 {
		t.Error("Unexpected expectedDirectoryCount", expectedDirectoryCount)
	}

	expectedDeletions := 1
	deleteNDirectories(expectedDeletions, t, initialDirectoryStructure, journal)
	expectedDirectoryCount = 1 + len(initialDirectoryStructure.Dirs) - expectedDeletions
	journal.Range(visitor)
	if expectedDirectoryCount != 0 {
		t.Error("Unexpected expectedDirectoryCount", expectedDirectoryCount)
	}
}

func TestJournalScannerInternals(t *testing.T) {
	type scannerTestStruct struct {
		txt          string
		recCnt       int
		failExpected bool
	}
	inputs := []scannerTestStruct{
		{"<dr>something</dr>", 1, false},
		{"<dr>\nsomething\n</dr>\n", 1, false},
		{"<dr>\nsomething\n</dr>\n\n", 1, false},
		{"<dr><bob>wibble</bob></dr>\n\n", 1, false},
		{"<dr>\nsomething\n</dr><dr>\nsomething\n</dr>\n", 2, false},
		{"<dr>\nsomething\n</dr>\nwibble wibble\n", 1, true},
	}
	for _, input := range inputs {
		expectedCnt := input.recCnt
		fc := func(record string) error {
			expectedCnt--
			t.Log("Got a record", record)
			return nil
		}
		err := slupReadFunc(strings.NewReader(input.txt), fc)
		if (err != nil) != (input.failExpected) {
			t.Error(err)
		}
		if expectedCnt != 0 {
			t.Error("Expected count issue on:", input, expectedCnt)
		}
	}
}

func TestJournalXmlIo(t *testing.T) {
	initialDirectoryStructure := populateDirectoryStuff(2, 5)
	initialDirectoryStructure.Dirs = []directoryTestStuff{
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
	}
	journal := createInitialJournal(t, &initialDirectoryStructure)
	var b bytes.Buffer
	err := journal.ToWriter(&b)
	if err != nil {
		t.Error(err)
	}
	journalTo := Journal{}
	err = journalTo.FromReader(&b)
	if err != nil {
		t.Error(err)
	}
	t.Log(journalTo)
	err = journal.Equals(journalTo, nil)
	if err != nil {
		t.Error(err)
	}
}
