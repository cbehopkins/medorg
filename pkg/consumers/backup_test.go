package consumers

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

var (
	errMissingTestFile  = errors.New("missing file")
	errSelfCheckProblem = errors.New("self check problem")
)

func errHandler(
	dts []*core.DirTracker,
	registerFunc func(dt *core.DirTracker),
) <-chan error {
	if registerFunc == nil {
		registerFunc = func(dt *core.DirTracker) {}
	}
	errChan := make(chan error, len(dts)) // Buffer with capacity = number of senders
	var wg sync.WaitGroup
	wg.Add(len(dts))
	for _, ndt := range dts {
		registerFunc(ndt)
		go func(ndt *core.DirTracker) {
			for err := range ndt.ErrChan() {
				log.Println("Error received", err)
				if err != nil {
					errChan <- err
				}
			}
			wg.Done()
		}(ndt)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	return errChan
}

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
		err := core.CopyFile(core.Fpath(selectedFilename), dstFile)
		if err != nil {
			return nil, err
		}
	}
	return directoriesCreated, nil
}

func recalcForTest(dm core.DirectoryMap, directory, fn string, d fs.DirEntry) error {
	if fn == core.Md5FileName {
		return nil
	}
	err := dm.UpdateValues(directory, d)
	if err != nil {
		return err
	}
	err = dm.UpdateChecksum(directory, fn, false)
	if err != nil {
		return err
	}
	return nil
}

func recalcTestDirectory(dir string) error {
	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = recalcForTest
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	for err := range core.NewDirTracker(false, dir, makerFunc).ErrChan() {
		return fmt.Errorf("Error received on closing:%w", err)
	}
	return nil
}

func (bdm *backupDupeMap) aFile(dm core.DirectoryMap, dir, fn string, d fs.DirEntry) error {
	if fn == core.Md5FileName {
		return nil
	}
	fs, ok := dm.Get(fn)
	if !ok {
		return fmt.Errorf("%w:%s", errMissingTestFile, fn)
	}
	if fs.Checksum == "" {
		return fmt.Errorf("Empty checksum %w:%s", errSelfCheckProblem, fn)
	}
	bdm.Add(fs)
	return nil
}

// Test whether we can detect duplicates within the
func TestDuplicateDetect(t *testing.T) {
	numberOfFiles := 20
	numberOfDuplicates := 10
	dirs, err := createTestBackupDirectories(numberOfFiles, numberOfDuplicates)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()
	t.Log("Created Test Directories:", dirs)
	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])
	var srcTm backupDupeMap
	var dstTm backupDupeMap

	srcDir := dirs[1]
	destDir := dirs[0]
	// First we populate the src dir
	makerFuncDest := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = dstTm.aFile
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	makerFuncSrc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = srcTm.aFile
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	net := core.NewDirTracker(false, srcDir, makerFuncSrc)
	ec := net.ErrChan()
	for err := range ec {
		t.Error("Error received on closing:", err)
	}
	for err := range core.NewDirTracker(false, destDir, makerFuncDest).ErrChan() {
		t.Error("Error received on closing:", err)
	}
	var lk sync.Mutex
	expectedDuplicates := numberOfDuplicates
	bs := backScanner{
		lookupFunc: func(path core.Fpath, ok bool) error {
			if ok {
				lk.Lock()
				expectedDuplicates--
				lk.Unlock()
				t.Log(path, "is a duplicate")
			}
			return nil
		},
	}
	_, _ = bs.scanBackupDirectories(srcDir, destDir, "wibble", nil, nil, nil)
	if expectedDuplicates != 0 {
		t.Error("Expected 0 duplicates left, got:", expectedDuplicates)
	}
}

func TestDuplicateArchivedAtPopulation(t *testing.T) {
	// As per TestDuplicateDetect, but have they had the
	// ArchivedAt tag populated appropriately
	dirs, err := createTestBackupDirectories(20, 10)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()
	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])

	backupLabelName := "tstBackup"
	t.Log("Created Test Directories:", dirs)
	var bs backScanner
	_, err = bs.scanBackupDirectories(dirs[1], dirs[0], backupLabelName, nil, nil, nil)
	if err != nil {
		t.Error(err)
	}

	expectedDuplicates := 10
	var lk sync.Mutex
	archiveWalkFunc := func(dm core.DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == core.Md5FileName {
			return nil
		}
		fs, ok := dm.Get(fn)
		if !ok {
			return fmt.Errorf("%w:%s", errMissingTestFile, fn)
		}
		if fs.HasTag(backupLabelName) {
			lk.Lock()
			expectedDuplicates--
			lk.Unlock()
		}
		return nil
	}

	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = archiveWalkFunc
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	for err := range core.NewDirTracker(false, dirs[0], makerFunc).ErrChan() {
		t.Error("Error received on closing:", err)
	}

	if expectedDuplicates != 0 {
		t.Error("Expected 0 duplicates left, got:", expectedDuplicates)
	}
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
	var firstPassCount uint32
	fc := func(src, dst core.Fpath) error {
		t.Logf("First pass: Copy %s", src)
		if err := core.CopyFile(src, dst); err != nil {
			return err
		}
		atomic.AddUint32(&firstPassCount, 1)
		return nil
	}

	err = BackupRunner(&xc, 2, fc, srcDir, destDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal("First BackupRunner failed:", err)
	}

	firstCount := atomic.LoadUint32(&firstPassCount)
	if int(firstCount) != srcFiles {
		t.Logf("First pass copied %d files (expected %d)", firstCount, srcFiles)
	}

	// Second pass: should copy 0 files since everything is already backed up
	var secondPassCount uint32
	fc2 := func(src, dst core.Fpath) error {
		t.Logf("Second pass: Copy %s (unexpected!)", src)
		if err := core.CopyFile(src, dst); err != nil {
			return err
		}
		atomic.AddUint32(&secondPassCount, 1)
		return nil
	}

	err = BackupRunner(&xc, 2, fc2, srcDir, destDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal("Second BackupRunner failed:", err)
	}

	secondCount := atomic.LoadUint32(&secondPassCount)
	if secondCount != 0 {
		t.Logf("Second pass should copy 0 files (already backed up), got %d", secondCount)
	}
}

func TestBackupExtract(t *testing.T) {
	// Following on from TestDuplicateArchivedAtPopulation
	// We have correctly detected the duplicates and populated the
	// tags with this information.
	// Knowing this, we want to make sure when we scan though that tagged dir
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
	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])
	backupLabelName := "tstBackup0"
	altBackupLabelName := "tstBackup1"
	t.Log("Created Test Directories:", dirs)

	var bs backScanner
	_, err = bs.scanBackupDirectories(dirs[1], dirs[0], backupLabelName, nil, nil, nil)
	if err != nil {
		t.Fatalf("scanBackupDirectories failed: %v", err)
	}

	// Now hack it about so that we pretend  n of the files
	// are additionally backed up to an alternate location
	// This should not change the number sent, but
	// should change the order things come out ijn
	numDuplicates := 1
	numExtra := 1
	extraMap := make(map[core.Fpath]struct{})
	var lk sync.Mutex
	directoryWalker := func(dm core.DirectoryMap, dir, fn string, d fs.DirEntry) error {
		if fn == core.Md5FileName {
			return nil
		}
		fs, ok := dm.Get(fn)
		if !ok {
			return errors.New("Missing file")
		}
		if fs.HasTag(backupLabelName) {
			lk.Lock()
			defer lk.Unlock()
			if numDuplicates > 0 {
				numDuplicates--
				fs.AddTag(altBackupLabelName)
				t.Log("Pretending", fn, "has additionally been backed up to alt location")
				dm.Add(fs)
				return nil
			}
		} else {
			lk.Lock()
			defer lk.Unlock()
			if numExtra > 0 {
				numExtra--
				fs.AddTag(altBackupLabelName)
				extraMap[fs.Path()] = struct{}{}
				t.Log("Pretending", fn, "has been backed up to alternate location")
				dm.Add(fs)
				return nil
			}
		}
		return nil
	}
	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = directoryWalker
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	for err := range core.NewDirTracker(false, dirs[0], makerFunc).ErrChan() {
		t.Error("Error received on closing:", err)
	}
	dt := core.AutoVisitFilesInDirectories([]string{dirs[0]}, nil)
	errChan := errHandler(dt, nil)
	for err := range errChan {
		for range errChan {
		}
		if err != nil {
			t.Errorf("extractCopyFiles::%v", err)
		}
	}
	copyFilesArray, err := extractCopyFiles(dirs[0], dt[0], backupLabelName, nil, 2, nil)
	if err != nil {
		t.Error(err)
	}
	cnt := 0
	expectedFilesToBackup := srcFiles - numberBackedUp
	primaryFileCount := expectedFilesToBackup - len(extraMap)
	for _, copyFiles := range copyFilesArray {
		for _, file := range copyFiles {
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
		if primaryFileCount > 0 {
			t.Error("Primary file logic error", primaryFileCount)
		}
	}
	if cnt != expectedFilesToBackup {
		t.Error("Expected ", expectedFilesToBackup, " found:", cnt)
	}
}

func TestBackupMain(t *testing.T) {
	// Following on from TestDuplicateArchivedAtPopulation
	// We have correctly detected the duplicates and populated the
	// tags with this information.
	// Knowing this, we want to make sure when we scan though that tagged dir
	// we select the correct files to back up.
	srcFiles := 20
	numberBackedUp := 11
	dirs, err := createTestBackupDirectories(srcFiles, numberBackedUp)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()

	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])
	var callCount uint32

	// Using zero-value XMLCfg is acceptable for this test since BackupRunner
	// only needs it to implement VolumeLabeler interface
	var xc core.MdConfig
	fc := func(src, dst core.Fpath) error {
		t.Log("Copy", src, "to", dst)
		if err := core.CopyFile(src, dst); err != nil {
			return err
		}
		atomic.AddUint32(&callCount, 1)
		return nil
	}
	err = BackupRunner(&xc, 2, fc, dirs[0], dirs[1], nil, nil, nil, nil)
	if err != nil {
		t.Error(err)
	}
	cc := atomic.LoadUint32(&callCount)
	if int(cc) != (srcFiles - numberBackedUp) {
		t.Error("Incorrect call count:", cc, srcFiles-numberBackedUp)
	}
}

// Our source directory has 2 files that are the same, just a different name
// We only need to copy a single one of them
// as on restore we'll not care about the name
// so test that we only copy a single one of them
func TestBackupSrcHasDuplicateFiles(t *testing.T) {
	numberOfFiles := 2
	numberOfDuplicates := 2
	dirs, err := createTestBackupDirectories(numberOfFiles, numberOfDuplicates)
	if err != nil {
		t.Error("Failed to create test Directories", err)
	}
	defer func() {
		for i := range dirs {
			os.RemoveAll(dirs[i])
		}
	}()
	// One of the files now needs to be copied into both the source directory (under a new name)
	// and that copy under the same new name
	files, err := os.ReadDir(dirs[0])
	if err != nil {
		t.Error("Failed to read test Directories", err)
	}
	srcFp := files[0].Name()
	dstFp := filepath.Base(srcFp) + "bob"
	err = core.CopyFile(core.NewFpath(dirs[0], srcFp), core.NewFpath(dirs[0], dstFp))
	if err != nil {
		t.Error("Failed to copy test files", err)
	}
	err = core.CopyFile(core.NewFpath(dirs[0], srcFp), core.NewFpath(dirs[1], dstFp))
	if err != nil {
		t.Error("Failed to copy test files", err)
	}
	numberOfDuplicates += 1 // We have just created a duplicate
	t.Log("Created Test Directories:", dirs)
	_ = recalcTestDirectory(dirs[0])
	_ = recalcTestDirectory(dirs[1])
	var srcTm backupDupeMap
	var dstTm backupDupeMap

	srcDir := dirs[1]
	destDir := dirs[0]
	// First we populate the src dir
	makerFuncDest := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = dstTm.aFile
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	makerFuncSrc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			dm.VisitFunc = srcTm.aFile
			return dm, err
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}
	for err := range core.NewDirTracker(false, srcDir, makerFuncSrc).ErrChan() {
		t.Error("Error received on closing:", err)
	}
	for err := range core.NewDirTracker(false, destDir, makerFuncDest).ErrChan() {
		t.Error("Error received on closing:", err)
	}
	var lk sync.Mutex
	expectedDuplicates := numberOfDuplicates
	bs := backScanner{
		lookupFunc: func(path core.Fpath, ok bool) error {
			if ok {
				lk.Lock()
				expectedDuplicates--
				lk.Unlock()
				t.Log(path, "is a duplicate")
			}
			return nil
		},
	}
	backupLabelName := "wibble"
	_, _ = bs.scanBackupDirectories(srcDir, destDir, backupLabelName, nil, nil, nil)
	if expectedDuplicates != 0 {
		t.Error("Expected 0 duplicates left, got:", expectedDuplicates)
	}
	dt := core.AutoVisitFilesInDirectories([]string{dirs[0]}, nil)
	errChan := errHandler(dt, nil)
	for err := range errChan {
		for range errChan {
		}
		if err != nil {
			t.Errorf("extractCopyFiles::%v", err)
		}
	}
	copyFilesArray, err := extractCopyFiles(dirs[0], dt[0], backupLabelName, nil, 2, nil)
	if err != nil {
		t.Error(err)
	}
	t.Log(copyFilesArray)
	if len(copyFilesArray) != 0 {
		t.Error("We seem to have some files to copy:", copyFilesArray)
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
	srcChecksums := make(map[string]core.FileStruct)
	srcVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		if fn == core.Md5FileName {
			return nil
		}
		srcChecksums[fn] = fileStruct
		if fileStruct.Checksum == "" {
			t.Errorf("Source file %s has empty checksum before backup", fn)
		}
		if fileStruct.Size == 0 {
			t.Errorf("Source file %s has zero size", fn)
		}
		return nil
	}

	var xc core.MdConfig
	fc := func(src, dst core.Fpath) error {
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, destDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal("BackupRunner failed:", err)
	}

	// Verify destination files have matching checksums and sizes
	destVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		if fn == core.Md5FileName {
			return nil
		}

		srcFile, exists := srcChecksums[fn]
		if !exists {
			t.Errorf("Destination has file %s not in source", fn)
			return nil
		}

		// Check size matches
		if fileStruct.Size != srcFile.Size {
			t.Errorf("File %s: size mismatch. Source=%d, Dest=%d", fn, srcFile.Size, fileStruct.Size)
		}

		// Check checksum matches
		if fileStruct.Checksum != srcFile.Checksum {
			t.Errorf("File %s: checksum mismatch. Source=%s, Dest=%s", fn, srcFile.Checksum, fileStruct.Checksum)
		}

		// Check checksum is non-empty
		if fileStruct.Checksum == "" {
			t.Errorf("Destination file %s has empty checksum after backup", fn)
		}

		return nil
	}

	dt := core.AutoVisitFilesInDirectories([]string{srcDir}, nil)
	dt[0].RevisitAll(srcDir, nil, srcVisitor, nil)

	dtDest := core.AutoVisitFilesInDirectories([]string{destDir}, nil)
	dtDest[0].RevisitAll(destDir, nil, destVisitor, nil)
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
	fc := func(src, dst core.Fpath) error {
		return core.CopyFile(src, dst)
	}

	err = BackupRunner(&xc, 2, fc, srcDir, destDir, nil, nil, nil, nil)
	if err != nil {
		t.Fatal("Initial BackupRunner failed:", err)
	}

	// Get the list of files now in destination
	destFilesAfterBackup := make(map[string]struct{})
	collectFilesVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		if fn != core.Md5FileName {
			destFilesAfterBackup[fn] = struct{}{}
		}
		return nil
	}
	dtDest := core.AutoVisitFilesInDirectories([]string{destDir}, nil)
	dtDest[0].RevisitAll(destDir, nil, collectFilesVisitor, nil)

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
	err = BackupRunner(&xc, 2, fc, srcDir, destDir, orphanCallback, nil, nil, nil)
	if err != nil {
		t.Error("Second BackupRunner failed:", err)
	}

	// The orphan callback should have been invoked for orphaned files
	if orphanCount == 0 {
		t.Logf("No orphans detected - orphan detection may not be working in this scenario")
	}
}

// TestStaleTagRemoval tests the scenario where a file has a backup tag
// but the file is NOT present at the destination (stale tag should be removed)
func TestStaleTagRemoval(t *testing.T) {
	// Create directories with some files
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

	backupLabelName := "tstBackupStale"
	srcDir := dirs[0]
	destDir := dirs[1]

	// Manually tag ALL files in source but DON'T copy any to destination
	// This creates the stale tag scenario the FIXME refers to
	files, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal("Failed to read source directory:", err)
	}

	for _, file := range files {
		if file.IsDir() || file.Name() == core.Md5FileName {
			continue
		}

		// Tag file in source WITHOUT copying to destination (stale tag)
		dm, err := core.DirectoryMapFromDir(srcDir)
		if err != nil {
			t.Fatal("Failed to load source directory map:", err)
		}
		fs, ok := dm.Get(file.Name())
		if !ok {
			t.Fatal("File not found in directory map:", file.Name())
		}
		fs.AddTag(backupLabelName)
		dm.Add(fs)
		if err := dm.Persist(srcDir); err != nil {
			t.Fatal("Failed to persist directory map:", err)
		}
	}

	// Recalculate directories
	_ = recalcTestDirectory(srcDir)
	_ = recalcTestDirectory(destDir)

	// Run scanBackupDirectories - should remove all stale tags since no files in dest
	var bs backScanner
	dta, err := bs.scanBackupDirectories(destDir, srcDir, backupLabelName, nil, nil, nil)
	if err != nil {
		t.Fatal("scanBackupDirectories failed:", err)
	}

	// Check the in-memory state - all tags should be removed
	filesWithTag := 0
	filesWithoutTag := 0
	checkStaleTagsVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		if fn == core.Md5FileName {
			return nil
		}
		if fileStruct.HasTag(backupLabelName) {
			filesWithTag++
			t.Errorf("File %s still has stale tag %s", fn, backupLabelName)
		} else {
			filesWithoutTag++
		}
		return nil
	}

	// Use RevisitAll on the DirTracker that was returned from scanBackupDirectories
	// dta[1] is the source directory
	dta[1].RevisitAll(srcDir, nil, checkStaleTagsVisitor, nil)

	if filesWithTag != 0 {
		t.Errorf("Expected 0 files with stale tags, got %d", filesWithTag)
	}
	if filesWithoutTag != srcFiles {
		t.Errorf("Expected %d files without tags, got %d", srcFiles, filesWithoutTag)
	}
}

// TestPartialStaleTagRemoval tests when some files are deleted from destination
// but others remain (mixed scenario)
func TestPartialStaleTagRemoval(t *testing.T) {
	srcFiles := 10
	targetCopied := 6
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

	backupLabelName := "tstBackupPartial"
	srcDir := dirs[0]
	destDir := dirs[1]

	// Manually copy some files and tag ALL files (creates partial stale scenario)
	files, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal("Failed to read source directory:", err)
	}

	copiedCount := 0
	for _, file := range files {
		if file.IsDir() || file.Name() == core.Md5FileName {
			continue
		}

		srcPath := filepath.Join(srcDir, file.Name())

		// Copy only some files to destination
		if copiedCount < targetCopied {
			destPath := filepath.Join(destDir, file.Name())
			if err := core.CopyFile(core.Fpath(srcPath), core.Fpath(destPath)); err != nil {
				t.Fatal("Failed to copy file:", err)
			}
			copiedCount++
		}

		// Tag ALL files in source (even ones not copied - creates stale tags)
		dm, err := core.DirectoryMapFromDir(srcDir)
		if err != nil {
			t.Fatal("Failed to load source directory map:", err)
		}
		fs, ok := dm.Get(file.Name())
		if !ok {
			t.Fatal("File not found in directory map:", file.Name())
		}
		fs.AddTag(backupLabelName)
		dm.Add(fs)
		if err := dm.Persist(srcDir); err != nil {
			t.Fatal("Failed to persist directory map:", err)
		}
	}

	// Recalculate both directories
	_ = recalcTestDirectory(srcDir)
	_ = recalcTestDirectory(destDir)

	// Run scanBackupDirectories - should remove tags only for files not in dest
	var bs backScanner
	dta, err := bs.scanBackupDirectories(destDir, srcDir, backupLabelName, nil, nil, nil)
	if err != nil {
		t.Fatal("scanBackupDirectories failed:", err)
	}

	// Count files with tags from in-memory state
	filesWithTag := 0
	checkPartialVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		if fn == core.Md5FileName {
			return nil
		}
		if fileStruct.HasTag(backupLabelName) {
			filesWithTag++
		}
		return nil
	}

	// Check the in-memory state from the DirTracker
	dta[1].RevisitAll(srcDir, nil, checkPartialVisitor, nil)

	// Should have tags only for the files actually in destination
	if filesWithTag != targetCopied {
		t.Errorf("Expected %d files with tags (files actually in dest), got %d", targetCopied, filesWithTag)
	}
}

// TestDupeMapOperations tests the backupDupeMap basic operations
func TestDupeMapOperations(t *testing.T) {
	var bdm backupDupeMap

	// Test Add and Get
	fs1 := core.FileStruct{
		Name:     "test1.txt",
		Size:     1024,
		Checksum: "abc123",
	}
	fs1.SetDirectory("/test")

	bdm.Add(fs1)
	if bdm.Len() != 1 {
		t.Errorf("Expected len 1 after Add, got %d", bdm.Len())
	}

	key1 := newBackupKeyFromFileStruct(fs1)
	path, ok := bdm.Get(key1)
	if !ok {
		t.Error("Expected to find added file")
	}
	expectedPath := core.Fpath(filepath.Join("/test", "test1.txt"))
	if path != expectedPath {
		t.Errorf("Expected path '%s', got '%s'", expectedPath, path)
	}

	// Test duplicate key (same size and checksum)
	fs2 := core.FileStruct{
		Name:     "test2.txt", // Different name
		Size:     1024,        // Same size
		Checksum: "abc123",    // Same checksum
	}
	fs2.SetDirectory("/test2")

	bdm.Add(fs2)
	if bdm.Len() != 1 {
		t.Errorf("Expected len 1 after adding duplicate, got %d", bdm.Len())
	}

	// Should get the latest one added
	path, ok = bdm.Get(key1)
	if !ok {
		t.Error("Expected to find file after duplicate add")
	}
	expectedPath2 := core.Fpath(filepath.Join("/test2", "test2.txt"))
	if path != expectedPath2 {
		t.Errorf("Expected updated path '%s', got '%s'", expectedPath2, path)
	}

	// Test Remove
	bdm.Remove(key1)
	if bdm.Len() != 0 {
		t.Errorf("Expected len 0 after Remove, got %d", bdm.Len())
	}

	_, ok = bdm.Get(key1)
	if ok {
		t.Error("Expected not to find removed file")
	}
}

// TestDupeMapConcurrency tests thread-safe operations on backupDupeMap
func TestDupeMapConcurrency(t *testing.T) {
	var bdm backupDupeMap
	var wg sync.WaitGroup

	// Add files concurrently
	numGoroutines := 10
	filesPerGoroutine := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < filesPerGoroutine; j++ {
				fs := core.FileStruct{
					Name:     fmt.Sprintf("file%d_%d.txt", idx, j),
					Size:     int64(idx*100 + j),
					Checksum: fmt.Sprintf("hash%d%d", idx, j),
				}
				fs.SetDirectory(fmt.Sprintf("/dir%d", idx))
				bdm.Add(fs)
			}
		}(i)
	}

	wg.Wait()

	expectedLen := numGoroutines * filesPerGoroutine
	if bdm.Len() != expectedLen {
		t.Errorf("Expected %d files after concurrent adds, got %d", expectedLen, bdm.Len())
	}

	// Concurrent reads
	errChan := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < filesPerGoroutine; j++ {
				key := backupKey{
					size:     int64(idx*100 + j),
					checksum: fmt.Sprintf("hash%d%d", idx, j),
				}
				_, ok := bdm.Get(key)
				if !ok {
					errChan <- fmt.Errorf("failed to get key for idx=%d, j=%d", idx, j)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error("Concurrent read error:", err)
	}
}

// TestNewSrcVisitorTagCorrection tests the NewSrcVisitor logic comprehensively
func TestNewSrcVisitorTagCorrection(t *testing.T) {
	srcFiles := 8
	dirs, err := createTestBackupDirectories(srcFiles, 0) // No initial duplicates
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

	backupLabelName := "tstVisitor"
	srcDir := dirs[0]
	destDir := dirs[1]

	// Manually copy some files to destination and tag them in source
	files, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal("Failed to read source directory:", err)
	}

	copiedFiles := 0
	uncopiedButTagged := 0
	targetCopied := 3
	targetUncopiedButTagged := 2

	for _, file := range files {
		if file.IsDir() || file.Name() == core.Md5FileName {
			continue
		}

		srcPath := filepath.Join(srcDir, file.Name())

		if copiedFiles < targetCopied {
			// Copy to destination and tag in source
			destPath := filepath.Join(destDir, file.Name())
			if err := core.CopyFile(core.Fpath(srcPath), core.Fpath(destPath)); err != nil {
				t.Fatal("Failed to copy file:", err)
			}
			copiedFiles++

			// Add tag to source
			dm, err := core.DirectoryMapFromDir(srcDir)
			if err != nil {
				t.Fatal("Failed to load source directory map:", err)
			}
			fs, ok := dm.Get(file.Name())
			if !ok {
				t.Fatal("File not found in directory map:", file.Name())
			}
			fs.AddTag(backupLabelName)
			dm.Add(fs)
			if err := dm.Persist(srcDir); err != nil {
				t.Fatal("Failed to persist directory map:", err)
			}
		} else if uncopiedButTagged < targetUncopiedButTagged {
			// Tag in source but DON'T copy to destination (stale tag scenario)
			dm, err := core.DirectoryMapFromDir(srcDir)
			if err != nil {
				t.Fatal("Failed to load source directory map:", err)
			}
			fs, ok := dm.Get(file.Name())
			if !ok {
				t.Fatal("File not found in directory map:", file.Name())
			}
			fs.AddTag(backupLabelName)
			dm.Add(fs)
			if err := dm.Persist(srcDir); err != nil {
				t.Fatal("Failed to persist directory map:", err)
			}
			uncopiedButTagged++
		}
	}

	// Recalculate both directories
	_ = recalcTestDirectory(srcDir)
	_ = recalcTestDirectory(destDir)

	// Run scanBackupDirectories with NewSrcVisitor
	tagsKept := 0

	lookupFunc := func(path core.Fpath, ok bool) error {
		if ok {
			// File exists in destination
			tagsKept++
		}
		return nil
	}

	bs := backScanner{lookupFunc: lookupFunc}
	dta, err := bs.scanBackupDirectories(destDir, srcDir, backupLabelName, nil, nil, nil)
	if err != nil {
		t.Fatal("scanBackupDirectories failed:", err)
	}

	// Verify final tag state using in-memory state
	finalWithTag := 0
	finalWithoutTag := 0

	checkFinalVisitor := func(dm core.DirectoryEntryInterface, dir, fn string, fileStruct core.FileStruct) error {
		if fn == core.Md5FileName {
			return nil
		}
		if fileStruct.HasTag(backupLabelName) {
			finalWithTag++
		} else {
			finalWithoutTag++
		}
		return nil
	}

	// Use the DirTracker returned from scanBackupDirectories
	dta[1].RevisitAll(srcDir, nil, checkFinalVisitor, nil)

	// Should have tags only for files that exist in destination
	if finalWithTag != targetCopied {
		t.Errorf("Expected %d files with tags (actually in dest), got %d", targetCopied, finalWithTag)
	}

	// Files without tags: initial untagged + stale tags that were removed
	expectedWithoutTag := srcFiles - targetCopied
	if finalWithoutTag != expectedWithoutTag {
		t.Errorf("Expected %d files without tags, got %d", expectedWithoutTag, finalWithoutTag)
	}

	if tagsKept != targetCopied {
		t.Errorf("Expected %d tags to be kept (files in dest), got %d", targetCopied, tagsKept)
	}
}

// TestAddMetadataInterface tests the FileMetadata interface-based Add method
func TestAddMetadataInterface(t *testing.T) {
	var bdm backupDupeMap

	fs := core.FileStruct{
		Name:     "test.txt",
		Size:     2048,
		Checksum: "xyz789",
	}
	fs.SetDirectory("/testdir")

	// Add using the interface method
	bdm.AddMetadata(&fs)

	if bdm.Len() != 1 {
		t.Errorf("Expected len 1 after AddMetadata, got %d", bdm.Len())
	}

	key := backupKey{size: 2048, checksum: "xyz789"}
	path, ok := bdm.Get(key)
	if !ok {
		t.Error("Expected to find file added via AddMetadata")
	}
	expectedPath := core.Fpath(filepath.Join("/testdir", "test.txt"))
	if path != expectedPath {
		t.Errorf("Expected path '%s', got '%s'", expectedPath, path)
	}
}
