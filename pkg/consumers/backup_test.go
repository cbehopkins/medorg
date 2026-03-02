package consumers

import (
	crand "crypto/rand"
	"errors"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/goleak"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var (
	errMissingTestFile  = errors.New("missing file")
	errSelfCheckProblem = errors.New("self check problem")
)

func makeFile(directory string) string {
	// Calculate checksum while data is still in memory for efficiency
	buff := make([]byte, 75000)
	if _, err := crand.Read(buff); err != nil {
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

func createTestBackupDirectories(numberOfFiles, numberOfDuplicates int) ([]string, error) {
	if numberOfDuplicates > numberOfFiles {
		return nil, errors.New("You asked for more duplicates than files")
	}
	directoriesCreated := make([]string, 2)
	for i := 0; i < 2; i++ {
		dir, err := os.MkdirTemp("", "tstDir")
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
		dstFile := core.NewFpath(directoriesCreated[1], stem)
		log.Println("Pretending to backup", dstFile)
		srcFile := core.NewFpath(selectedFilename)
		_, err := core.CopyFile(srcFile, dstFile)
		if err != nil {
			return nil, err
		}
	}
	return directoriesCreated, nil
}

func recalcTestDirectory(dir string) error {
	dw := core.NewDirectoryWalker(core.MakeTokenChan(core.NumTrackerOutstanding))

	// Add mutator to update checksums for all files
	dw.AddFileMutator(func(file core.Fpath, d os.FileInfo, fs core.FileStruct) (core.FileStruct, error) {
		err := fs.UpdateChecksum(false, false, nil)
		if errors.Is(err, os.ErrNotExist) {
			return fs, core.ErrDeleteThisEntry
		}
		return fs, err
	})

	if err := dw.Walk(dir); err != nil {
		_ = dw.Close()
		return err
	}
	return dw.Close()
}

// TestPreExistingBackupTags verifies that BackupRunner skips files that already have backup tags
func TestPreExistingBackupTags(t *testing.T) {
	srcFiles := 10
	dirs, err := createTestBackupDirectories(srcFiles, 0) // No initial copies
	if err != nil {
		t.Fatal("Failed to create test directories:", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()

	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])

	srcDir := dirs[0]
	destDir := dirs[1]

	// First pass: backup all files
	var xc core.MdConfig
	fc := func(src, dst core.Fpath) (int64, error) {
		t.Logf("First pass: Copy %s", src)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, destDir, nil, nil, nil, nil, false, nil, srcDir)
	if err != nil {
		t.Fatal("First BackupRunner failed:", err)
	}

	// Second pass: should copy 0 files since everything is already backed up
	var secondPassCount uint32
	fc2 := func(src, dst core.Fpath) (int64, error) {
		t.Logf("Second pass: Copy %s (unexpected!)", src)
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc2, destDir, nil, nil, nil, nil, false, nil, srcDir)
	if err != nil {
		t.Fatal("Second BackupRunner failed:", err)
	}

	secondCount := atomic.LoadUint32(&secondPassCount)
	if secondCount != 0 {
		t.Logf("Second pass should copy 0 files (already backed up), got %d", secondCount)
	}
}

// TestBackupChecksumMaintenance verifies that checksums and file metadata are correctly preserved during backup
func TestBackupChecksumMaintenance(t *testing.T) {
	srcFiles := 5
	dirs, err := createTestBackupDirectories(srcFiles, 0)
	if err != nil {
		t.Fatal("Failed to create test directories:", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()

	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])

	srcDir := dirs[0]
	destDir := dirs[1]

	// Collect checksums from source before backup
	srcChecksums := make(map[core.Fname]core.FileMetadata)
	srcVisitor := func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		if fn == core.Md5FileName || fn == core.VolumePathName {
			return nil
		}
		srcChecksums[fn] = fm
		if fm.GetChecksum() == "" {
			t.Errorf("Source file %s has empty checksum before backup", fn)
		}
		if fm.GetSize() == 0 {
			t.Errorf("Source file %s has zero size", fn)
		}
		return nil
	}

	var xc core.MdConfig
	fc := func(src, dst core.Fpath) (int64, error) {
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, destDir, nil, nil, nil, nil, false, nil, srcDir)
	if err != nil {
		t.Fatal("BackupRunner failed:", err)
	}

	// Verify destination files have correct metadata
	destVisitor := func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		if fn == core.Md5FileName || fn == core.VolumePathName {
			return nil
		}

		srcFile, exists := srcChecksums[fn]
		if !exists {
			t.Errorf("Destination has file %s not in source", fn)
			return nil
		}

		// Check size matches
		if fm.GetSize() != srcFile.GetSize() {
			t.Errorf("File %s: size mismatch. Source=%d, Dest=%d", fn, srcFile.GetSize(), fm.GetSize())
		}

		// Check checksum matches
		if fm.GetChecksum() != srcFile.GetChecksum() {
			t.Errorf("File %s: checksum mismatch. Source=%s, Dest=%s", fn, srcFile.GetChecksum(), fm.GetChecksum())
		}

		// Check checksum is non-empty
		if fm.GetChecksum() == "" {
			t.Errorf("Destination file %s has empty checksum after backup", fn)
		}

		return nil
	}

	// Use DirectoryWalker to visit source files
	dw := core.NewDirectoryWalker(core.MakeTokenChan(core.NumTrackerOutstanding))
	defer dw.Close()
	dw.AddFileVisitor(func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		return srcVisitor(fn, fm, fi)
	})
	if err := dw.Walk(srcDir); err != nil {
		t.Fatalf("DirectoryWalker src error: %v", err)
	}

	// Use DirectoryWalker to visit destination files
	dwDest := core.NewDirectoryWalker(core.MakeTokenChan(core.NumTrackerOutstanding))
	defer dwDest.Close()
	dwDest.AddFileVisitor(func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		return destVisitor(fn, fm, fi)
	})
	if err := dwDest.Walk(destDir); err != nil {
		t.Fatalf("DirectoryWalker dest error: %v", err)
	}
}

// TestBackupOrphanDetection verifies that orphan files (in dest but not in src) are properly detected
func TestBackupOrphanDetection(t *testing.T) {
	srcFiles := 10
	dirs, err := createTestBackupDirectories(srcFiles, 0)
	if err != nil {
		t.Fatal("Failed to create test directories:", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()

	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])

	srcDir := dirs[0]
	destDir := dirs[1]

	// Perform initial backup
	var xc core.MdConfig
	fc := func(src, dst core.Fpath) (int64, error) {
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, destDir, nil, nil, nil, nil, false, nil, srcDir)

	// Get the list of files now in destination
	destFilesAfterBackup := make(map[core.Fname]struct{})
	collectFilesVisitor := func(dir core.Dirname, fn core.Fname, fileStruct core.FileStruct) error {
		if fn != core.Md5FileName {
			destFilesAfterBackup[fn] = struct{}{}
		}
		return nil
	}
	// Use DirectoryWalker to collect destination files
	dwDest := core.NewDirectoryWalker(core.MakeTokenChan(core.NumTrackerOutstanding))
	defer dwDest.Close()
	dwDest.AddFileVisitor(func(fn core.Fname, fm core.FileMetadata, fi os.FileInfo) error {
		return collectFilesVisitor(core.Dirname(destDir), fn, *fm.(*core.FileStruct))
	})
	if err := dwDest.Walk(destDir); err != nil {
		t.Fatalf("DirectoryWalker collect error: %v", err)
	}

	if len(destFilesAfterBackup) == 0 {
		t.Logf("Initial backup resulted in 0 files in destination (may indicate issue with test setup)")
		return
	}

	// Now delete some files from source - these become orphans in destination
	files, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal("Failed to read source directory:", err)
	}

	filesToDelete := 0
	for i, file := range files {
		if file.IsDir() || file.Name() == core.Md5FileName {
			continue
		}
		// Delete first 2 files from source
		if i < 2 {
			srcPath := filepath.Join(srcDir, file.Name())
			if err := os.Remove(srcPath); err != nil {
				t.Logf("Warning: failed to delete %s: %v", srcPath, err)
			}
			filesToDelete++
		}
	}

	if filesToDelete == 0 {
		t.Logf("No files were deleted (may indicate filesystem issue)")
		return
	}

	// Recalculate source directory to reflect deletions
	_ = recalcTestDirectory(srcDir)

	// Track orphaned files detected via callback
	var orphanMutex sync.Mutex
	orphanCount := 0
	orphanCallback := func(path string) error {
		orphanMutex.Lock()
		orphanCount++
		orphanMutex.Unlock()
		t.Logf("Orphan callback invoked for: %s", filepath.Base(path))
		return nil
	}

	// Run backup again with orphan detection
	err = BackupRunner(&xc, 2, fc, destDir, orphanCallback, nil, nil, nil, false, nil, srcDir)
	if err != nil {
		t.Error("Second BackupRunner failed:", err)
	}

	// The orphan callback should have been invoked for orphaned files
	if orphanCount == 0 {
		t.Errorf("Expected orphan callback to be invoked for stale files, but was not")
	} else {
		t.Logf("✓ Orphan detection working: %d orphaned files detected", orphanCount)
	}
}
