package medorg

import (
	"fmt"
	"path/filepath"
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
		dm := NewDirectoryMap()
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
func TestJournalBasicXml(t *testing.T) {
	td0 := populateDirectoryStuff(2, 5)
	td0.Dirs = []directoryTestStuff{
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
		populateDirectoryStuff(1, 5),
	}
	t.Log("Working with:", td0)
	journal := Journal{}

	visitFuncInitial0 := func(de DirectoryEntry) error {
		err := journal.AppendJournalFromDm(de.dm, de.dir)
		if err == errFileExistsInJournal {
			return fmt.Errorf("initial setup TestJournalBasicXml %w,%s", err, de.dir)
		}
		return err
	}
	// Let's get ourselves an initial setup
	runDirectory(t, &td0, visitFuncInitial0)
	t.Log(journal)

	// Now we want to pretend we are doing a walk, and all is unchanged
	visitFuncRevisit := func(de DirectoryEntry) error {
		err := journal.AppendJournalFromDm(de.dm, de.dir)
		if err != errFileExistsInJournal {
			t.Log("Got:", de.dm)
			return fmt.Errorf("issue on revisit %w,%s", err, de.dir)
		}
		return nil
	}
	// Here we are going for a walk over the the test data
	// Nothing exists on disk in this test, but journal should report
	// having seen this already
	runDirectory(t, &td0, visitFuncRevisit)

	// Now let's add a few files
	// This should be a new directory with 3 files
	numFiles := 3
	numDirs := 1
	td1 := populateDirectoryStuff(numDirs, numFiles)
	visitFuncInitial1 := func(de DirectoryEntry) error {
		err := journal.AppendJournalFromDm(de.dm, de.dir)
		if err == errFileExistsInJournal {
			return fmt.Errorf("initial1 TestJournalBasicXml %w,%s", err, de.dir)
		}
		numDirs--
		return err
	}
	runDirectory(t, &td1, visitFuncInitial1)
	if numDirs != 0 {
		t.Error("Strange number of directories", numDirs)
	}

	// Now what happens if we send the same directory names, but with different files in them
	// We should see that they are treated the same as new directories
	// dm, ok := td0.de.dm.(DirectoryMap)
	// if !ok {
	// 	t.Error("Unable to cast spell")
	// }
	// changesToMake := 1
	// changeArray := make([]string, changesToMake)
	// for filename := range dm.mp {
	// 	if changesToMake <= 0 {
	// 		break
	// 	}
	// 	changeArray[changesToMake-1] = filename
	// }
	// for _, v := range changeArray {
	// 	newFilename := v + "_mod"
	// 	dm.mp[newFilename] = dm.mp[v]
	// 	delete(dm.mp, v)
	// }

	expectedAdditions := 0
	visitFuncAdd0 := func(de DirectoryEntry) error {
		err := journal.AppendJournalFromDm(de.dm, de.dir)
		if err == errFileExistsInJournal {
			return nil
		}
		if err == nil {
			expectedAdditions--
		}
		return err
	}
	runDirectory(t, &td0, visitFuncAdd0)
	if expectedAdditions != 0 {
		t.Error("Strange number of expectedAdditions", expectedAdditions)
	}

	// Now we need to test removing some directories

}
