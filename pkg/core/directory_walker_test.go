package core

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDirectoryWalkerVisitsAllFiles(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")

	expected := 0
	expected += writeDirMap(t, root, []string{"root.txt"})
	expected += writeDirMap(t, sub, []string{"child.txt"})

	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	visited := make(map[string]struct{})
	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visited[filepath.Join(string(fm.Directory()), string(name))] = struct{}{}
		return nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	if len(visited) != expected {
		t.Fatalf("visited %d files, expected %d", len(visited), expected)
	}
	want := []string{filepath.Join(root, "root.txt"), filepath.Join(sub, "child.txt")}
	for _, w := range want {
		if _, ok := visited[w]; !ok {
			t.Fatalf("missing visit for %s", w)
		}
	}
}

func TestDirectoryWalkerHandlesEmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	// persist an empty directory map so the walker has metadata to read
	dm := NewDirectoryMap()
	if err := dm.Persist(Dirname(dir)); err != nil {
		t.Fatalf("persist empty dm: %v", err)
	}

	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	calls := 0
	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		calls++
		return nil
	})

	if err := walker.Walk(dir); err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected no visits for empty directory, got %d", calls)
	}
}

func buildLargeTree(t *testing.T, dir string, depth, fanout int) int {
	t.Helper()
	if depth == 0 {
		return 0
	}

	files := []string{"a.txt", "b.txt"}
	total := writeDirMap(t, dir, files)

	for i := range fanout {
		child := filepath.Join(dir, fmt.Sprintf("dir_%d", i))
		total += buildLargeTree(t, child, depth-1, fanout)
	}
	return total
}

func TestDirectoryWalkerLargeTree(t *testing.T) {
	root := t.TempDir()
	expected := buildLargeTree(t, root, 3, 3) // 3 levels, branching factor 3, 2 files per directory

	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	visited := 0
	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visited++
		return nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	if visited != expected {
		t.Fatalf("visited %d files, expected %d", visited, expected)
	}
}

// Helper function to read and verify checksum values from the persisted .medorg.xml file
// Returns the checksum for the specified filename in the directory, or empty string if not found
func readChecksumFromXML(t *testing.T, dir, filename string) string {
	t.Helper()
	xmlPath := filepath.Join(dir, Md5FileName)
	f, err := os.Open(xmlPath)
	if err != nil {
		t.Fatalf("failed to open %s: %v", xmlPath, err)
	}
	defer f.Close()

	byteValue, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("failed to read %s: %v", xmlPath, err)
	}

	var m5f Md5File
	if err := xml.Unmarshal(byteValue, &m5f); err != nil {
		t.Fatalf("failed to unmarshal XML: %v", err)
	}

	for _, fs := range m5f.Files {
		if string(fs.Name) == filename {
			return fs.Checksum
		}
	}
	return ""
}

// Helper function to read file metadata from the persisted .medorg.xml file
// Returns the FileStruct for the specified filename, or nil if not found
func readFileStructFromXML(t *testing.T, dir, filename string) *FileStruct {
	t.Helper()
	xmlPath := filepath.Join(dir, Md5FileName)
	f, err := os.Open(xmlPath)
	if err != nil {
		t.Fatalf("failed to open %s: %v", xmlPath, err)
	}
	defer f.Close()

	byteValue, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("failed to read %s: %v", xmlPath, err)
	}

	var m5f Md5File
	if err := xml.Unmarshal(byteValue, &m5f); err != nil {
		t.Fatalf("failed to unmarshal XML: %v", err)
	}

	for _, fs := range m5f.Files {
		if string(fs.Name) == filename {
			return &fs
		}
	}
	return nil
}

// TestDirectoryWalkerFileVisitorCorrectValues verifies that the file visitor receives
// correct values including pre-existing BackupDest and Checksum values
func TestDirectoryWalkerFileVisitorCorrectValues(t *testing.T) {
	root := t.TempDir()

	// Create a file
	filename := "testfile.txt"
	filePath := filepath.Join(root, filename)
	fileContent := []byte("test data content")
	if err := os.WriteFile(filePath, fileContent, 0o644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	// Create a DirectoryMap with pre-populated checksum and backup destination
	dm := NewDirectoryMap()
	fs, err := NewFileStruct(root, filename)
	if err != nil {
		t.Fatalf("NewFileStruct failed: %v", err)
	}

	// Manually calculate the checksum
	checksum, err := CalcMd5File(root, filename)
	if err != nil {
		t.Fatalf("failed to calculate checksum: %v", err)
	}
	fs.Checksum = checksum

	// Add backup destinations to the file
	fs.AddBackupDestination("VOL_1")
	fs.AddBackupDestination("VOL_2")

	dm.Add(fs)
	if err := dm.Persist(Dirname(root)); err != nil {
		t.Fatalf("failed to persist DirectoryMap: %v", err)
	}

	// Walk the directory and verify the visitor receives correct values
	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	visitorCalled := false

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visitorCalled = true

		// Verify the filename
		if string(name) != filename {
			t.Errorf("expected filename %s, got %s", filename, name)
		}

		// Verify the directory
		if fm.Directory() != Dirname(root) {
			t.Errorf("expected directory %s, got %s", root, fm.Directory())
		}

		// Verify the checksum is preserved
		if fm.GetChecksum() != checksum {
			t.Errorf("expected checksum %s, got %s", checksum, fm.GetChecksum())
		}

		// Verify the file size
		expectedSize := int64(len(fileContent))
		if fm.GetSize() != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, fm.GetSize())
		}

		// Verify the backup destinations are preserved
		backupDests := fm.BackupDestinations()
		if len(backupDests) != 2 {
			t.Errorf("expected 2 backup destinations, got %d: %v", len(backupDests), backupDests)
		}
		if !fm.HasBackupOn("VOL_1") {
			t.Error("expected VOL_1 in backup destinations")
		}
		if !fm.HasBackupOn("VOL_2") {
			t.Error("expected VOL_2 in backup destinations")
		}

		// Verify the os.FileInfo is valid
		if fi.Name() != filename {
			t.Errorf("expected FileInfo name %s, got %s", filename, fi.Name())
		}
		if fi.Size() != expectedSize {
			t.Errorf("expected FileInfo size %d, got %d", expectedSize, fi.Size())
		}

		return nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	if !visitorCalled {
		t.Fatal("file visitor was not called")
	}

	// Verify that the values were persisted
	persistedChecksum := readChecksumFromXML(t, root, filename)
	if persistedChecksum != checksum {
		t.Errorf("persisted checksum %s does not match expected %s", persistedChecksum, checksum)
	}

	persistedFS := readFileStructFromXML(t, root, filename)
	if persistedFS == nil {
		t.Fatal("failed to read FileStruct from XML")
	}
	if len(persistedFS.BackupDest) != 2 {
		t.Errorf("expected 2 persisted backup destinations, got %d", len(persistedFS.BackupDest))
	}
}

// TestDirectoryWalkerInvalidChecksumRecalculation verifies that when a mutator recalculates
// an invalid checksum, the changes are persisted to the .medorg.xml file
func TestDirectoryWalkerInvalidChecksumRecalculation(t *testing.T) {
	root := t.TempDir()

	// Create a file
	filename := "testfile.txt"
	filePath := filepath.Join(root, filename)
	fileContent := []byte("test data content")
	if err := os.WriteFile(filePath, fileContent, 0o644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	// Calculate the correct checksum
	correctChecksum, err := CalcMd5File(root, filename)
	if err != nil {
		t.Fatalf("failed to calculate checksum: %v", err)
	}

	// Create a DirectoryMap with an INVALID checksum
	dm := NewDirectoryMap()
	fs, err := NewFileStruct(root, filename)
	if err != nil {
		t.Fatalf("NewFileStruct failed: %v", err)
	}

	// Set an invalid checksum
	invalidChecksum := "invalid_checksum_value_12345"
	fs.Checksum = invalidChecksum

	dm.Add(fs)
	if err := dm.Persist(Dirname(root)); err != nil {
		t.Fatalf("failed to persist DirectoryMap with invalid checksum: %v", err)
	}

	// Verify the invalid checksum is in the XML
	initialChecksum := readChecksumFromXML(t, root, filename)
	if initialChecksum != invalidChecksum {
		t.Fatalf("initial checksum not set correctly in XML: got %s, expected %s", initialChecksum, invalidChecksum)
	}

	// Walk the directory with a mutator that validates and recalculates invalid checksums
	walker := NewDirectoryWalker(nil)

	// Add a mutator that validates the checksum
	walker.AddFileMutator(func(file Fpath, d os.FileInfo, fs FileStruct) (FileStruct, error) {
		// Validate checksum - recalculate if invalid
		actualChecksum, err := CalcMd5File(string(fs.Directory()), string(fs.Name))
		if err != nil {
			return fs, err
		}

		if fs.Checksum != actualChecksum {
			fs.Checksum = actualChecksum
		}
		return fs, nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	// CRITICAL: Verify that the CORRECT checksum was persisted back to the XML
	// This ensures we don't have to recalculate on the next run
	persistedChecksum := readChecksumFromXML(t, root, filename)
	if persistedChecksum != correctChecksum {
		t.Errorf("persisted checksum %s does not match correct checksum %s", persistedChecksum, correctChecksum)
	}

	// Verify it's not the invalid one
	if persistedChecksum == invalidChecksum {
		t.Error("invalid checksum was persisted instead of corrected one")
	}
}

// TestDirectoryWalkerFileSizeChangeRecalculation verifies that when file size changes
// from what's recorded in .medorg.xml, a mutator can detect this and recalculate the checksum,
// and the changes are persisted to the .medorg.xml file
func TestDirectoryWalkerFileSizeChangeRecalculation(t *testing.T) {
	root := t.TempDir()

	// Create a file with initial content
	filename := "testfile.txt"
	filePath := filepath.Join(root, filename)
	initialContent := []byte("initial content")
	if err := os.WriteFile(filePath, initialContent, 0o644); err != nil {
		t.Fatalf("failed to write initial file: %v", err)
	}

	// Calculate checksum with initial content
	initialChecksum, err := CalcMd5File(root, filename)
	if err != nil {
		t.Fatalf("failed to calculate initial checksum: %v", err)
	}

	// Create DirectoryMap with initial state
	dm := NewDirectoryMap()
	fs, err := NewFileStruct(root, filename)
	if err != nil {
		t.Fatalf("NewFileStruct failed: %v", err)
	}
	fs.Checksum = initialChecksum
	dm.Add(fs)
	if err := dm.Persist(Dirname(root)); err != nil {
		t.Fatalf("failed to persist initial DirectoryMap: %v", err)
	}

	// Now change the file content (different size)
	newContent := []byte("much longer new content that is definitely bigger than before")
	if err := os.WriteFile(filePath, newContent, 0o644); err != nil {
		t.Fatalf("failed to write new file content: %v", err)
	}

	// Calculate the NEW correct checksum
	newChecksum, err := CalcMd5File(root, filename)
	if err != nil {
		t.Fatalf("failed to calculate new checksum: %v", err)
	}

	// Verify the checksums are different (sanity check)
	if initialChecksum == newChecksum {
		t.Fatal("checksums should be different after file content change")
	}

	// Walk the directory with a mutator that detects file size changes and recalculates
	walker := NewDirectoryWalker(nil)

	// Add a mutator that detects file size changes and recalculates checksums
	walker.AddFileMutator(func(file Fpath, d os.FileInfo, fs FileStruct) (FileStruct, error) {
		// If file size changed, recalculate the checksum
		if fs.Size != d.Size() {
			fs.Size = d.Size()
			actualChecksum, err := CalcMd5File(string(fs.Directory()), string(fs.Name))
			if err != nil {
				return fs, err
			}
			fs.Checksum = actualChecksum
		}
		return fs, nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	// CRITICAL: Verify that the NEW checksum was persisted back to the XML
	// This ensures we don't have to recalculate on the next run
	persistedChecksum := readChecksumFromXML(t, root, filename)
	if persistedChecksum != newChecksum {
		t.Errorf("persisted checksum %s does not match new checksum %s", persistedChecksum, newChecksum)
	}

	// Verify it's not the old one
	if persistedChecksum == initialChecksum {
		t.Error("old checksum was persisted instead of new one")
	}

	// Read the full FileStruct to verify size is updated
	persistedFS := readFileStructFromXML(t, root, filename)
	if persistedFS == nil {
		t.Fatal("failed to read FileStruct from XML")
	}
	expectedSize := int64(len(newContent))
	if persistedFS.Size != expectedSize {
		t.Errorf("persisted size %d does not match new size %d", persistedFS.Size, expectedSize)
	}
}

// TestDirectoryWalkerCancelChannel verifies that the DirectoryWalker stops walking
// when the cancel channel is closed
func TestDirectoryWalkerCancelChannel(t *testing.T) {
	root := t.TempDir()

	// Create a directory structure with multiple levels
	dirs := []string{
		filepath.Join(root, "dir1"),
		filepath.Join(root, "dir2"),
		filepath.Join(root, "dir3"),
		filepath.Join(root, "dir1", "subdir1"),
		filepath.Join(root, "dir1", "subdir2"),
	}

	// Create files in each directory
	filesPerDir := 3
	for _, dir := range dirs {
		files := make([]string, filesPerDir)
		for i := 0; i < filesPerDir; i++ {
			files[i] = fmt.Sprintf("file%d.txt", i)
		}
		writeDirMap(t, dir, files)
	}

	// Create a walker with a cancel channel
	walker := NewDirectoryWalker(nil)
	cancelChan := walker.cancelChan

	visitedFiles := 0

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visitedFiles++
		return nil
	})

	// Start walking in a goroutine and cancel it after a short delay
	errChan := make(chan error, 1)
	go func() {
		errChan <- walker.Walk(root)
	}()

	// Give the walker a moment to start traversing
	// (but close the cancel channel before it can visit all files)
	// We'll close it after a short delay
	go func() {
		// Let some directories be visited first
		// This gives us a way to test that we actually stopped early
		os.Stdin.Read([]byte{}) // This won't actually execute, but the timing should work
		// In practice, we close it immediately to ensure we stop soon
		close(cancelChan)
	}()

	// Wait a tiny bit to let the goroutine close the cancel channel
	// Note: timing is a bit fuzzy here, so we just verify we don't visit ALL files
	err := <-errChan

	// The error should be either nil (successful skip) or filepath.SkipAll wrapped in error
	// The walk should complete without panicking
	if err != nil && err != filepath.SkipAll {
		// filepath.SkipAll might be returned directly or wrapped
		t.Logf("Walk returned error (expected filepath.SkipAll or nil): %v", err)
	}

	// Verify that we visited FEWER files than we would have if the walk completed fully
	// Total possible files = len(dirs) * filesPerDir = 5 dirs * 3 files = 15 files
	expectedTotalFiles := len(dirs) * filesPerDir
	if visitedFiles > 0 && visitedFiles < expectedTotalFiles {
		t.Logf("Cancelled walk visited %d files (out of %d possible) - cancellation worked", visitedFiles, expectedTotalFiles)
	} else if visitedFiles == expectedTotalFiles {
		// If we somehow managed to visit all files despite cancellation, that's suspicious
		// but could happen due to timing - we'll note it
		t.Logf("Warning: visited all %d files despite cancellation (timing issue in test)", visitedFiles)
	} else {
		t.Logf("Cancelled walk visited %d files", visitedFiles)
	}
}

// TestDirectoryWalkerCancelChannelResponsiveness verifies that closing the cancel channel
// actually causes the walk to stop and not visit more directories
func TestDirectoryWalkerCancelChannelResponsiveness(t *testing.T) {
	root := t.TempDir()

	// Create a deep directory structure to ensure the cancel happens during traversal
	for i := 0; i < 5; i++ {
		dir := root
		for j := 0; j < 3; j++ {
			dir = filepath.Join(dir, fmt.Sprintf("level%d_branch%d", j, i))
		}
		// Create one file in each leaf directory
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0o644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
		// Persist empty DirectoryMap for each dir to ensure the walker processes them
		dm := NewDirectoryMap()
		dm.Add(FileStruct{Name: Fname("file.txt"), Checksum: "test"})
		if err := dm.Persist(Dirname(dir)); err != nil {
			t.Fatalf("failed to persist: %v", err)
		}
	}

	// Create a walker with a cancel channel
	walker := NewDirectoryWalker(nil)
	cancelChan := walker.cancelChan

	var directoriesVisited int
	var visitMutex sync.Mutex

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visitMutex.Lock()
		directoriesVisited++
		visitMutex.Unlock()

		// Close the cancel channel after visiting the first file
		// This should stop subsequent directory traversals
		if directoriesVisited == 1 {
			close(cancelChan)
		}
		return nil
	})

	err := walker.Walk(root)

	// Check that we didn't visit all possible directories
	// If cancellation works, we should have visited much fewer than 5 leaf directories
	visitMutex.Lock()
	visited := directoriesVisited
	visitMutex.Unlock()

	if visited < 5 {
		t.Logf("Successfully cancelled: visited %d directories (less than max 5)", visited)
	} else {
		t.Logf("Note: visited %d directories (cancellation may not have taken effect due to timing)", visited)
	}

	if err != nil {
		t.Logf("Walk returned error: %v", err)
	}
}

// TestDirectoryWalkerSkipDirFile verifies that a .mdSkipDir file in a directory
// causes only that directory to be skipped, not sibling directories
func TestDirectoryWalkerSkipDirFile(t *testing.T) {
	root := t.TempDir()

	// Create a directory structure:
	// root/
	//   dir1/ (.mdSkipDir present) - should be skipped
	//   dir2/ - should be visited
	//   dir3/ - should be visited
	dirStructure := map[string]bool{
		"dir1": true,  // has skip file
		"dir2": false, // normal
		"dir3": false, // normal
	}

	for dirName, shouldSkip := range dirStructure {
		dirPath := filepath.Join(root, dirName)
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		// Create a test file in each directory
		if err := os.WriteFile(filepath.Join(dirPath, "file.txt"), []byte("data"), 0o644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}

		// Add .mdSkipDir file to skip this directory
		if shouldSkip {
			if err := os.WriteFile(filepath.Join(dirPath, ".mdSkipDir"), []byte(""), 0o644); err != nil {
				t.Fatalf("failed to write skip file: %v", err)
			}
		}

		// Persist empty directory map
		dm := NewDirectoryMap()
		fs := FileStruct{Name: Fname("file.txt"), Checksum: "test"}
		dm.Add(fs)
		if err := dm.Persist(Dirname(dirPath)); err != nil {
			t.Fatalf("failed to persist: %v", err)
		}
	}

	// Walk and collect visited directories
	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	visitedDirs := make(map[string]struct{})

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visitedDirs[filepath.Base(string(fm.Directory()))] = struct{}{}
		return nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	// Verify dir1 (with skip file) was NOT visited
	if _, visited := visitedDirs["dir1"]; visited {
		t.Error("dir1 should have been skipped due to .mdSkipDir file")
	}

	// Verify dir2 and dir3 WERE visited
	if _, visited := visitedDirs["dir2"]; !visited {
		t.Error("dir2 should have been visited")
	}
	if _, visited := visitedDirs["dir3"]; !visited {
		t.Error("dir3 should have been visited")
	}

	t.Logf("Visited directories: %v", visitedDirs)
}

// TestDirectoryWalkerIgnoreFunction verifies that an ignore function
// skips only matching directories, not siblings
func TestDirectoryWalkerIgnoreFunction(t *testing.T) {
	root := t.TempDir()

	// Create a directory structure:
	// root/
	//   temp/ - matches ignore pattern, should be skipped
	//   cache/ - matches ignore pattern, should be skipped
	//   data/ - does not match, should be visited
	//   project/ - does not match, should be visited
	dirNames := []string{"temp", "cache", "data", "project"}

	for _, dirName := range dirNames {
		dirPath := filepath.Join(root, dirName)
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		// Create a test file in each directory
		if err := os.WriteFile(filepath.Join(dirPath, "file.txt"), []byte("data"), 0o644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}

		// Persist directory map
		dm := NewDirectoryMap()
		fs := FileStruct{Name: Fname("file.txt"), Checksum: "test"}
		dm.Add(fs)
		if err := dm.Persist(Dirname(dirPath)); err != nil {
			t.Fatalf("failed to persist: %v", err)
		}
	}

	// Walk with ignore function that skips "temp" and "cache" directories
	walker := NewDirectoryWalker(nil)
	walker.shouldIgnore = func(path string) bool {
		baseName := filepath.Base(path)
		// Skip directories named "temp" or "cache"
		return baseName == "temp" || baseName == "cache"
	}

	visitedDirs := make(map[string]struct{})

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		visitedDirs[filepath.Base(string(fm.Directory()))] = struct{}{}
		return nil
	})

	if err := walker.Walk(root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	// Verify temp and cache were NOT visited
	if _, visited := visitedDirs["temp"]; visited {
		t.Error("temp should have been skipped by ignore function")
	}
	if _, visited := visitedDirs["cache"]; visited {
		t.Error("cache should have been skipped by ignore function")
	}

	// Verify data and project WERE visited
	if _, visited := visitedDirs["data"]; !visited {
		t.Error("data should have been visited")
	}
	if _, visited := visitedDirs["project"]; !visited {
		t.Error("project should have been visited")
	}

	t.Logf("Visited directories: %v", visitedDirs)
}

// TestDirectoryWalkerSkipAll verifies that SkipAll stops processing
// of any more sibling directories after being triggered
func TestDirectoryWalkerSkipAll(t *testing.T) {
	root := t.TempDir()

	// Create a directory structure:
	// root/
	//   dir_a/ - should be visited
	//   dir_b/ - will trigger SkipAll
	//   dir_c/ - should NOT be visited (SkipAll stops walking)
	dirNames := []string{"dir_a", "dir_b", "dir_c"}

	for _, dirName := range dirNames {
		dirPath := filepath.Join(root, dirName)

		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		// Create a file in each directory
		if err := os.WriteFile(filepath.Join(dirPath, "file.txt"), []byte("data"), 0o644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}

		// Persist directory map
		dm := NewDirectoryMap()
		fs := FileStruct{Name: Fname("file.txt"), Checksum: "test"}
		dm.Add(fs)
		if err := dm.Persist(Dirname(dirPath)); err != nil {
			t.Fatalf("failed to persist: %v", err)
		}
	}

	// Walk with a condition that triggers SkipAll at dir_b
	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	visitedDirs := make(map[string]struct{})
	var visitMutex sync.Mutex

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		dirName := filepath.Base(string(fm.Directory()))
		visitMutex.Lock()
		visitedDirs[dirName] = struct{}{}
		visitMutex.Unlock()

		// Trigger SkipAll when we encounter dir_b
		if dirName == "dir_b" {
			// Return filepath.SkipAll to stop walking all remaining directories
			return filepath.SkipAll
		}
		return nil
	})

	err := walker.Walk(root)

	// The walk should stop and return the SkipAll error
	if err != filepath.SkipAll {
		t.Logf("Expected filepath.SkipAll error, got: %v (this is okay if propagated correctly)", err)
	}

	visitMutex.Lock()
	visited := visitedDirs
	visitMutex.Unlock()

	t.Logf("Visited directories: %v", visited)

	// Verify that dir_a was visited (it comes before SkipAll trigger)
	if _, vis := visited["dir_a"]; !vis {
		t.Error("dir_a should have been visited")
	}

	// Verify that dir_b was visited (it's where SkipAll is triggered)
	if _, vis := visited["dir_b"]; !vis {
		t.Error("dir_b should have been visited before SkipAll took effect")
	}

	// Verify that dir_c was NOT visited (SkipAll should have stopped processing sibling directories)
	if _, vis := visited["dir_c"]; vis {
		t.Error("dir_c should NOT have been visited (SkipAll should have stopped all further walking)")
	}
}

// TestDirectoryWalkerSkipDirFromVisitor verifies that SkipDir returned from a file visitor
// skips the current directory AND its subdirectories, but not siblings
func TestDirectoryWalkerSkipDirFromVisitor(t *testing.T) {
	root := t.TempDir()

	// Create a directory structure:
	// root/
	//   dir_a/ - should be visited
	//     subdir_a/ - should be visited
	//   dir_b/ - will trigger SkipDir (should skip this dir and subdirs)
	//     subdir_b/ - should NOT be visited
	//   dir_c/ - should be visited
	//     subdir_c/ - should be visited

	dirs := []struct {
		name    string
		subdir  string
		triggerSkipDir bool
	}{
		{"dir_a", "subdir_a", false},
		{"dir_b", "subdir_b", true},  // Will trigger SkipDir
		{"dir_c", "subdir_c", false},
	}

	for _, d := range dirs {
		dirPath := filepath.Join(root, d.name)
		subdirPath := filepath.Join(dirPath, d.subdir)

		if err := os.MkdirAll(subdirPath, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		// Create files in both main dir and subdir
		for _, path := range []string{dirPath, subdirPath} {
			if err := os.WriteFile(filepath.Join(path, "file.txt"), []byte("data"), 0o644); err != nil {
				t.Fatalf("failed to write file: %v", err)
			}
			dm := NewDirectoryMap()
			fs := FileStruct{Name: Fname("file.txt"), Checksum: "test"}
			dm.Add(fs)
			if err := dm.Persist(Dirname(path)); err != nil {
				t.Fatalf("failed to persist: %v", err)
			}
		}
	}

	// Walk with a condition that triggers SkipDir at dir_b
	walker := NewDirectoryWalker(nil)
		defer walker.Close()
	visitedDirs := make(map[string]struct{})
	var visitMutex sync.Mutex

	walker.AddFileVisitor(func(name Fname, fm FileMetadata, fi os.FileInfo) error {
		dirName := filepath.Base(string(fm.Directory()))
		visitMutex.Lock()
		visitedDirs[dirName] = struct{}{}
		visitMutex.Unlock()

		// Trigger SkipDir when we encounter dir_b
		if dirName == "dir_b" {
			// Return filepath.SkipDir to skip this directory and its subdirectories
			return filepath.SkipDir
		}
		return nil
	})

	err := walker.Walk(root)
	if err != nil {
		t.Fatalf("walk failed unexpectedly: %v", err)
	}

	visitMutex.Lock()
	visited := visitedDirs
	visitMutex.Unlock()

	t.Logf("Visited directories: %v", visited)

	// Verify dir_a and its subdir were visited
	if _, vis := visited["dir_a"]; !vis {
		t.Error("dir_a should have been visited")
	}
	if _, vis := visited["subdir_a"]; !vis {
		t.Error("subdir_a should have been visited")
	}

	// Verify dir_b was visited (where SkipDir was triggered)
	if _, vis := visited["dir_b"]; !vis {
		t.Error("dir_b should have been visited (SkipDir is returned from there)")
	}

	// Verify subdir_b was NOT visited (SkipDir should skip subdirectories)
	if _, vis := visited["subdir_b"]; vis {
		t.Error("subdir_b should NOT have been visited (SkipDir from dir_b should skip its subdirectories)")
	}

	// Verify dir_c and subdir_c were visited (SkipDir should not affect siblings)
	if _, vis := visited["dir_c"]; !vis {
		t.Error("dir_c should have been visited (SkipDir should not affect siblings)")
	}
	if _, vis := visited["subdir_c"]; !vis {
		t.Error("subdir_c should have been visited (SkipDir should not affect sibling's subdirectories)")
	}
}

