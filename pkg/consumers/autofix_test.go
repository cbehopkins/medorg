package consumers

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestRename0(t *testing.T) {
	testMode = true
	DomainList := []string{
		"(.*)_calc",
	}

	AF := NewAutoFix(DomainList)
	fs := core.FileStruct{Name: "test_calc.flv"}
	fs, mod := AF.CheckRename(fs)
	if mod {
		t.Fatal("Modified while disabled", fs)
	} else {
		if fs.Name != "test_calc.flv" {
			t.Fatal("Name was modified", fs)
		}
	}
	AF.RenameFiles = true
	fs, mod = AF.CheckRename(fs)
	if mod {
		log.Println("FS is now", fs)
	} else {
		t.Fatal("Not modified", fs)
	}
}

type renameStruct struct {
	In     string
	Out    string
	Modify bool
}

func TestRename1(t *testing.T) {
	testMode = true
	DomainList := []string{
		"(.*)_calc",
		"(.*)_bob_(.*)",
	}
	testStruct := []renameStruct{
		{"test_calc.flv", "test.flv", true},
		{"test_calc.flv.flv", "test.flv", true},
		{"test_calc.mp4.flv", "test.flv", true},
		{"test_calc", "test_calc", false},
		{"test_bob_c.mpg", "testc.mpg", true},
		{"test_calc_bob.jpg", "test.jpg", true},
		{"Party.mp4.mp4", "Party.mp4", true},
		{"This is a - weird filename.wmv.mp4", "This is a - weird filename.mp4", true},
		{"fred.jpg.doc", "fred.jpg.doc", false},
		{"/wibble.com_4cbb7934338409b928a4ee6b86725738.mp4.mp4", "/wibble.com_4cbb7934338409b928a4ee6b86725738.mp4", true},
	}
	AF := NewAutoFix(DomainList)
	AF.RenameFiles = true
	var mod bool
	var fs core.FileStruct
	for _, ts := range testStruct {
		fn0 := ts.In
		fn1 := ts.Out
		// Create a synthetic FileStruct for testing (testMode = true means files don't exist on disk)
		fs = core.FileStruct{Name: fn0}
		fs.SetDirectory(".")

		fs, mod = AF.CheckRename(fs)
		if mod == ts.Modify {
			if fs.Name == fn1 {
				log.Println("FS is now", fn0, fn1)
			} else {
				t.Fatal("Incorrectly modified:", fn0, fn1, fs.Name)
			}
		} else {
			t.Fatal("Not modified", fn0, fn1, fs.Name)
		}
	}
}

// TestWkFunZeroLengthFile tests WkFun handling of zero-length files
func TestWkFunZeroLengthFile(t *testing.T) {
	testMode = true
	dm := core.NewDirectoryMap()

	// Add a zero-length file
	zeroFile := core.FileStruct{
		Name:     "empty.txt",
		Size:     0,
		Checksum: "d41d8cd98f00b204e9800998ecf8427e", // MD5 of empty file
	}
	zeroFile.SetDirectory("/test")
	dm.Add(zeroFile)

	// Test without delete enabled
	af := NewAutoFix([]string{})
	af.SilenceLogging = true

	err := af.WkFun(*dm, "/test", "empty.txt", nil)
	if err != nil {
		t.Fatalf("WkFun failed for zero-length file without delete: %v", err)
	}

	// File should still exist
	if _, ok := dm.Get("empty.txt"); !ok {
		t.Fatal("Zero-length file was deleted when DeleteFiles=false")
	}

	// Test with delete enabled
	af.DeleteFiles = true
	err = af.WkFun(*dm, "/test", "empty.txt", nil)
	if err != nil {
		t.Fatalf("WkFun failed for zero-length file with delete: %v", err)
	}

	// File should be removed
	if _, ok := dm.Get("empty.txt"); ok {
		t.Fatal("Zero-length file was not deleted when DeleteFiles=true")
	}
}

// TestWkFunNonExistentFile tests error handling for files not in DirectoryMap
func TestWkFunNonExistentFile(t *testing.T) {
	testMode = true
	dm := core.NewDirectoryMap()
	af := NewAutoFix([]string{})

	err := af.WkFun(*dm, "/test", "nonexistent.txt", nil)
	if err == nil {
		t.Fatal("Expected error when WkFun called with non-existent file")
	}
	expectedMsg := "asked to update a file that does not exist"
	if err.Error() != expectedMsg {
		t.Fatalf("Expected error '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestWkFunRenameFile tests file renaming functionality
func TestWkFunRenameFile(t *testing.T) {
	testMode = true
	DomainList := []string{
		"(.*)_calc",
	}

	dm := core.NewDirectoryMap()

	// Add a file that should be renamed
	fs := core.FileStruct{
		Name:     "video_calc.mp4",
		Size:     1024,
		Checksum: "abcd1234",
	}
	fs.SetDirectory("/test")
	dm.Add(fs)

	af := NewAutoFix(DomainList)
	af.RenameFiles = true
	af.SilenceLogging = true

	err := af.WkFun(*dm, "/test", "video_calc.mp4", nil)
	if err != nil {
		t.Fatalf("WkFun failed for rename: %v", err)
	}

	// Check that the file was renamed in the hash map
	key := autoFixKey{size: 1024, checksum: "abcd1234"}
	af.FhLock.RLock()
	renamedFs, ok := af.FileHash[key]
	af.FhLock.RUnlock()

	if !ok {
		t.Fatal("File not found in hash map after rename")
	}

	if renamedFs.Name != "video.mp4" {
		t.Fatalf("File was not renamed correctly, expected 'video.mp4', got '%s'", renamedFs.Name)
	}
}

// TestWkFunDuplicateDetection tests duplicate file detection
func TestWkFunDuplicateDetection(t *testing.T) {
	testMode = true
	dm := core.NewDirectoryMap()

	// First file
	fs1 := core.FileStruct{
		Name:     "original.jpg",
		Size:     2048,
		Checksum: "hash123",
	}
	fs1.SetDirectory("/test/favs")
	dm.Add(fs1)

	// Second file with same size and checksum (duplicate)
	fs2 := core.FileStruct{
		Name:     "duplicate.jpg",
		Size:     2048,
		Checksum: "hash123",
	}
	fs2.SetDirectory("/test/to")
	dm.Add(fs2)

	af := NewAutoFix([]string{})
	af.SilenceLogging = true

	// Process first file
	err := af.WkFun(*dm, "/test/favs", "original.jpg", nil)
	if err != nil {
		t.Fatalf("WkFun failed for first file: %v", err)
	}

	// Process duplicate
	err = af.WkFun(*dm, "/test/to", "duplicate.jpg", nil)
	if err != nil {
		t.Fatalf("WkFun failed for duplicate file: %v", err)
	}

	// Check that the favs file is preferred (higher score)
	key := autoFixKey{size: 2048, checksum: "hash123"}
	af.FhLock.RLock()
	finalFs := af.FileHash[key]
	af.FhLock.RUnlock()

	// The file from favs should be kept
	if finalFs.Directory() != "/test/favs" {
		t.Fatalf("Expected file from /test/favs to be kept, got %s", finalFs.Directory())
	}
}

// TestWkFunMultipleFiles tests processing multiple files with different characteristics
func TestWkFunMultipleFiles(t *testing.T) {
	testMode = true
	dm := core.NewDirectoryMap()

	files := []core.FileStruct{
		{Name: "file1.jpg", Size: 1000, Checksum: "aaa"},
		{Name: "file2.mp4", Size: 2000, Checksum: "bbb"},
		{Name: "file3.gif", Size: 1000, Checksum: "ccc"},
		{Name: "file4.wmv", Size: 3000, Checksum: "ddd"},
	}

	for i := range files {
		files[i].SetDirectory("/test")
		dm.Add(files[i])
	}

	af := NewAutoFix([]string{})
	af.SilenceLogging = true

	// Process all files
	for _, fs := range files {
		err := af.WkFun(*dm, "/test", fs.Name, nil)
		if err != nil {
			t.Fatalf("WkFun failed for %s: %v", fs.Name, err)
		}
	}

	// Check that all files are in the hash map
	af.FhLock.RLock()
	hashMapSize := len(af.FileHash)
	af.FhLock.RUnlock()

	if hashMapSize != len(files) {
		t.Fatalf("Expected %d files in hash map, got %d", len(files), hashMapSize)
	}

	// Verify each file is accessible by its key
	for _, fs := range files {
		key := autoFixKey{size: fs.Size, checksum: fs.Checksum}
		af.FhLock.RLock()
		_, ok := af.FileHash[key]
		af.FhLock.RUnlock()
		if !ok {
			t.Fatalf("File %s not found in hash map", fs.Name)
		}
	}
}

// TestWkFunUnknownExtension tests handling of files with unrecognized extensions
func TestWkFunUnknownExtension(t *testing.T) {
	testMode = true
	dm := core.NewDirectoryMap()

	// File with unknown extension
	fs := core.FileStruct{
		Name:     "document.txt",
		Size:     500,
		Checksum: "xyz789",
	}
	fs.SetDirectory("/test")
	dm.Add(fs)

	af := NewAutoFix([]string{"(.*)_test"})
	af.RenameFiles = true
	af.SilenceLogging = true

	err := af.WkFun(*dm, "/test", "document.txt", nil)
	if err != nil {
		t.Fatalf("WkFun failed for unknown extension: %v", err)
	}

	// File should still be in hash map even though it wasn't renamed
	key := autoFixKey{size: 500, checksum: "xyz789"}
	af.FhLock.RLock()
	storedFs, ok := af.FileHash[key]
	af.FhLock.RUnlock()

	if !ok {
		t.Fatal("File with unknown extension not stored in hash map")
	}

	if storedFs.Name != "document.txt" {
		t.Fatalf("File name should not change for unknown extension, got %s", storedFs.Name)
	}
}

// TestWkFunConcurrentAccess tests thread-safety of WkFun
func TestWkFunConcurrentAccess(t *testing.T) {
	testMode = true
	dm := core.NewDirectoryMap()

	// Add multiple files (start from 1 to avoid size 0)
	for i := 1; i <= 10; i++ {
		fs := core.FileStruct{
			Name:     fmt.Sprintf("file%d.jpg", i),
			Size:     int64(i * 100),
			Checksum: fmt.Sprintf("hash%d", i),
		}
		fs.SetDirectory("/test")
		dm.Add(fs)
	}

	af := NewAutoFix([]string{})
	af.SilenceLogging = true

	// Process files concurrently
	errChan := make(chan error, 10)
	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			fileName := fmt.Sprintf("file%d.jpg", idx)
			err := af.WkFun(*dm, "/test", fileName, nil)
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		t.Fatalf("Concurrent WkFun call failed: %v", err)
	}

	// Verify all files are in hash map
	af.FhLock.RLock()
	hashMapSize := len(af.FileHash)
	af.FhLock.RUnlock()

	if hashMapSize != 10 {
		t.Fatalf("Expected 10 files in hash map after concurrent access, got %d", hashMapSize)
	}
}
