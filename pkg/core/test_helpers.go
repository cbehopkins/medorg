package core

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func recalcForTest(dm DirectoryMap, directory Dirname, fn Fname, d fs.DirEntry) error {
	if string(fn) == Md5FileName {
		return nil
	}
	err := dm.UpdateValues(directory, d)
	if err != nil {
		return err
	}
	err = dm.UpdateChecksum(string(directory), string(fn), false)
	return err
}

func recalcTestDirectory(dir string) error {
	makerFunc := func(dir string) (DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (DirectoryEntryInterface, error) {
			dm, err := DirectoryMapFromDir(Dirname(dir))
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
func makeFile(directory string) string {
	buff := make([]byte, 75000)
	if _, err := rand.Read(buff); err != nil {
		panic(err)
	}
	tmpfile, err := os.CreateTemp(directory, "example")
	if err != nil {
		panic(err)
	}
	if _, err := tmpfile.Write(buff); err != nil {
		panic(err)
	}
	if err := tmpfile.Close(); err != nil {
		panic(err)
	}
	return tmpfile.Name()
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
	for range numberOfDirectoriesWide {
		runningCnt += numberOfFiles
		if numberOfDirectoriesDeep > 0 {
			runningCnt += moveDetectDirCreationCount(numberOfDirectoriesWide, numberOfDirectoriesDeep-1, numberOfFiles)
		}
	}
	return runningCnt
}

// makeDir creates a directory (including parents) with standard permissions.
func makeDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

// writeFile writes content to a file with standard permissions.
func writeFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0o644)
}

// createTestDirectoriesWithFs builds a deterministic test tree at root.
// It creates dir_{i} children and file_{j}.txt files under each.
func createTestDirectoriesWithFs(root string, numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles int) error {
	return makeTestFilesAndDirectoriesWithFs(root, numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles)
}

// makeTestFilesAndDirectoriesWithFs recursively creates test files and subdirectories.
func makeTestFilesAndDirectoriesWithFs(directory string, numberOfDirectoriesWide, numberOfDirectoriesDeep, numberOfFiles int) error {
	for i := 0; i < numberOfDirectoriesWide; i++ {
		dirName := filepath.Join(directory, fmt.Sprintf("dir_%d", i))
		if err := makeDir(dirName); err != nil {
			return err
		}

		for j := 0; j < numberOfFiles; j++ {
			fileName := filepath.Join(dirName, fmt.Sprintf("file_%d.txt", j))
			content := []byte(fmt.Sprintf("test content for file %d in dir %d", j, i))
			if err := writeFile(fileName, content); err != nil {
				return err
			}
		}

		if numberOfDirectoriesDeep > 0 {
			if err := makeTestFilesAndDirectoriesWithFs(dirName, numberOfDirectoriesWide, numberOfDirectoriesDeep-1, numberOfFiles); err != nil {
				return err
			}
		}
	}
	return nil
}

// helper to create files and persist a directory map for a directory
func writeDirMap(t *testing.T, dir string, files []string) int {
	t.Helper()
	dm := NewDirectoryMap()
	count := 0
	for _, name := range files {
		// ensure directory exists
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		fp := filepath.Join(dir, name)
		if err := os.WriteFile(fp, []byte("data"), 0o644); err != nil {
			t.Fatalf("write file %s: %v", fp, err)
		}
		fs, err := NewFileStruct(dir, name)
		if err != nil {
			t.Fatalf("NewFileStruct for %s: %v", fp, err)
		}
		dm.Add(fs)
		count++
	}
	if err := dm.Persist(Dirname(dir)); err != nil {
		t.Fatalf("persist dm for %s: %v", dir, err)
	}
	return count
}

type mockDtType struct {
	errChan chan error
	lock    *sync.RWMutex
	closed  *bool
	visiter func(Dirname, Fname)
}

func newMockDtType() (mdt mockDtType) {
	mdt.errChan = make(chan error)
	mdt.lock = new(sync.RWMutex)
	mdt.closed = new(bool)
	return
}

func (mdt mockDtType) ErrChan() <-chan error {
	return mdt.errChan
}

func (mdt mockDtType) Start() error {
	return nil
}

func (mdt mockDtType) Close() {
	mdt.lock.Lock()
	*mdt.closed = true
	mdt.lock.Unlock()
	close(mdt.errChan)
}

var errTestChanClosed = errors.New("visit called to a closed structure")

func (mdt mockDtType) Visitor(dir Dirname, file Fname, d fs.DirEntry) error {
	mdt.lock.RLock()
	closed := *mdt.closed
	mdt.lock.RUnlock()
	if closed {
		return fmt.Errorf("%w at %s/%s", errTestChanClosed, string(dir), string(file))
	}
	if mdt.visiter != nil {
		mdt.visiter(dir, file)
	}
	return nil
}

func (mdt mockDtType) VisitFile(dir, file string, d fs.DirEntry, callback func()) {
	mdt.lock.Lock()
	if *mdt.closed {
		mdt.errChan <- fmt.Errorf("%w at %s/%s", errTestChanClosed, dir, file)
	}
	mdt.lock.Unlock()

	if mdt.visiter != nil {
		mdt.visiter(Dirname(dir), Fname(file))
	}
	callback()
}

func (dt mockDtType) Revisit(dir string, fileVisitor func(dm DirectoryEntryInterface, dir, fn string, fileStruct FileStruct) error) error {
	return nil
}
