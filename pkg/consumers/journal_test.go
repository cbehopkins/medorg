package consumers

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

type directoryTestStuff struct {
	Name   string
	De     *core.DirectoryEntry
	Dirs   []directoryTestStuff
	Files  []core.FileStruct
	parent *directoryTestStuff
}

// FullPath returns the complete path by walking up the parent chain
func (dts *directoryTestStuff) FullPath() string {
	if dts.parent == nil {
		return dts.Name
	}
	return filepath.Join(dts.parent.FullPath(), dts.Name)
}

// populateDirectoryStuff create a test directory
// that has fileCnt files nested under a directory dirDepth deep
func populateDirectoryStuff(dirDepth, fileCnt int) directoryTestStuff {
	randDirectory := func(len int) string {
		paths := make([]string, len)
		for i := range paths {
			paths[i] = core.RandStringBytesMaskImprSrcSB(6)
		}
		return filepath.Join(paths...)
	}
	dts := directoryTestStuff{
		Name: randDirectory(dirDepth),
	}
	dts.Files = make([]core.FileStruct, fileCnt)
	for i := 0; i < fileCnt; i++ {
		dts.Files[i] = core.FileStruct{Name: core.RandStringBytesMaskImprSrcSB(5), Checksum: core.RandStringBytesMaskImprSrcSB(8)}
	}
	return dts
}

func directoryMapFromStuff(path string, dts *directoryTestStuff, deChan chan<- core.DirectoryEntry) error {
	mkFk := func(path string) (core.DirectoryEntryInterface, error) {
		dm := core.NewDirectoryMap()
		for _, fs := range dts.Files {
			// Add add multiple would be good for later
			dm.Add(fs)
		}
		return dm, nil
	}
	if dts.De == nil {
		de, err := core.NewDirectoryEntry(filepath.Join(path, dts.Name), mkFk)
		if err != nil {
			return err
		}
		dts.De = &de
	}
	deChan <- *dts.De
	for i := range dts.Dirs {
		// Link parent for FullPath()
		dts.Dirs[i].parent = dts
		err := directoryMapFromStuff(dts.Name, &dts.Dirs[i], deChan)
		if err != nil {
			return err
		}
	}
	return nil
}

func runDirectory(t *testing.T, dts *directoryTestStuff, visitFunc func(de core.DirectoryEntry) error) {
	deChan := make(chan core.DirectoryEntry)
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
	dm, ok := dts.De.Dm.(*core.DirectoryMap)
	if !ok {
		return errors.New("Unable to cast spell")
	}

	return dm.ForEachFileMod(func(filename string, fm core.FileMetadata) (string, core.FileMetadata, error) {
		if changesToMake > 0 {
			changesToMake--
			return filename + "_mod", fm, nil
		}
		return filename, fm, nil
	})
}

func createInitialJournal(t *testing.T, initialDirectoryStructure *directoryTestStuff) *Journal {
	journal := NewJournal(100)
	visitFuncInitial0 := func(de core.DirectoryEntry) error {
		dm, ok := de.Dm.(core.DirectoryEntryJournalableInterface)
		if !ok {
			return fmt.Errorf("Dm does not implement DirectoryEntryJournalableInterface for dir %s", de.Dir)
		}
		err := journal.AppendJournalFromDm(dm, de.Dir)
		if err == ErrFileExistsInJournal {
			return fmt.Errorf("initial setup TestJournalDummyWalks %w,%s", err, de.Dir)
		}
		return err
	}
	// Startoff with an initial directory and get a Journal
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

	visitFuncRevisit := func(de core.DirectoryEntry) error {
		dm, ok := de.Dm.(core.DirectoryEntryJournalableInterface)
		if !ok {
			return fmt.Errorf("Dm does not implement DirectoryEntryJournalableInterface for dir %s", de.Dir)
		}
		err := journal.AppendJournalFromDm(dm, de.Dir)
		if err != ErrFileExistsInJournal {
			t.Log("Got:", de.Dm)
			return fmt.Errorf("issue on revisit %w,%s", err, de.Dir)
		}
		return nil
	}
	// Here we are going for a walk over the the test data
	// Nothing exists on disk in this test, but Journal should report
	// having seen this already
	runDirectory(t, &initialDirectoryStructure, visitFuncRevisit)
	journal.Flush()
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
	visitFuncInitial1 := func(de core.DirectoryEntry) error {
		dm, ok := de.Dm.(core.DirectoryEntryJournalableInterface)
		if !ok {
			return fmt.Errorf("Dm does not implement DirectoryEntryJournalableInterface for dir %s", de.Dir)
		}
		err := journal.AppendJournalFromDm(dm, de.Dir)
		if err == ErrFileExistsInJournal {
			return fmt.Errorf("initial1 TestJournalBasicXml %w,%s", err, de.Dir)
		}
		numDirsToAdd--
		return err
	}
	td1 := populateDirectoryStuff(numDirsToAdd, numFilesToAdd)
	runDirectory(t, &td1, visitFuncInitial1)
	if numDirsToAdd != 0 {
		t.Error("Strange number of directories", numDirsToAdd)
	}
	journal.Flush()
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

	visitFuncAdd0 := func(de core.DirectoryEntry) error {
		dm, ok := de.Dm.(core.DirectoryEntryJournalableInterface)
		if !ok {
			return fmt.Errorf("Dm does not implement DirectoryEntryJournalableInterface for dir %s", de.Dir)
		}
		err := journal.AppendJournalFromDm(dm, de.Dir)
		if err == ErrFileExistsInJournal {
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
	journal.Flush()
}

func deleteNDirectories(n int, t *testing.T, initialDirectoryStructure directoryTestStuff, journal *Journal) {
	// Delete the first n directories from the structure
	if n > len(initialDirectoryStructure.Dirs) {
		t.Errorf("Cannot delete %d directories, only %d available", n, len(initialDirectoryStructure.Dirs))
		return
	}

	// Process each directory to be deleted
	for i := 0; i < n; i++ {
		bob := initialDirectoryStructure.Dirs[i]
		bob.parent = &initialDirectoryStructure
		deletedDirectory := directoryTestStuff{
			Name: bob.FullPath(),
		}
		processSingleDeletion(t, &deletedDirectory, journal)
	}
}

func processSingleDeletion(t *testing.T, deletedDirectory *directoryTestStuff, journal *Journal) {
	t.Helper()

	visitFuncDeleter := func(de core.DirectoryEntry) error {
		dm, ok := de.Dm.(core.DirectoryEntryJournalableInterface)
		if !ok {
			return fmt.Errorf("Dm does not implement DirectoryEntryJournalableInterface for dir %s", de.Dir)
		}
		err := journal.AppendJournalFromDm(dm, de.Dir)
		if err == ErrFileExistsInJournal {
			// All files should already exist
			return nil
		}
		if err == nil {
			return errors.New("all files should exist")
		}
		return err
	}
	runDirectory(t, deletedDirectory, visitFuncDeleter)
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
	visitFuncCheck := func(de core.DirectoryEntry) error {
		dm, ok := de.Dm.(core.DirectoryEntryJournalableInterface)
		if !ok {
			return fmt.Errorf("Dm does not implement DirectoryEntryJournalableInterface for dir %s", de.Dir)
		}
		err := journal.AppendJournalFromDm(dm, de.Dir)
		if err == ErrFileExistsInJournal {
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
	journal.Flush()
}

// TestJournalDummyVisitDirs will test that the Journal records what we expect
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
	visitor := func(de core.DirectoryEntryJournalableInterface, dir string) error {
		expectedDirectoryCount--
		if de.Len() != 5 {
			t.Error("Wrong File count for:", de)
		}
		return nil
	}
	if err := journal.Range(visitor); err != nil {
		t.Error("Range error:", err)
	}
	if expectedDirectoryCount != 0 {
		t.Error("Unexpected expectedDirectoryCount", expectedDirectoryCount)
	}

	expectedDeletions := 1
	deleteNDirectories(expectedDeletions, t, initialDirectoryStructure, journal)
	expectedDirectoryCount = 1 + len(initialDirectoryStructure.Dirs) - expectedDeletions
	if err := journal.Range(visitor); err != nil {
		t.Error("Range error:", err)
	}
	if expectedDirectoryCount != 0 {
		t.Error("Unexpected expectedDirectoryCount", expectedDirectoryCount)
	}
	journal.Flush()
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
		err := SlupReadFunc(strings.NewReader(input.txt), fc)
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
	journal := NewJournal(100)
	defer journal.Flush()
	var b bytes.Buffer
	err := journal.ToWriter(&b)
	if err != nil {
		t.Error(err)
	}
	journalTo := NewJournal(100)
	defer journalTo.Flush()
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
