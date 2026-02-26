package consumers

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// backupProcessorInterface defines common interface for both implementations
type backupProcessorInterface interface {
	addSrcFile(md5Key string, size int64, backupDest []string, file core.Fpath) error
	addDstFile(md5Key string, size int64, backupDest []string, file core.Fpath) error
	prioritizedSrcFiles(maxNumBackups int) (func(yield func(fileData) bool), error)
}

// closeable extends the interface for processors that need cleanup
type closeable interface {
	Close() error
}

// testProcessor wraps a processor with cleanup
type testProcessor struct {
	processor backupProcessorInterface
	cleanup   func()
}

// getTestProcessors returns test instances of both implementations
func getTestProcessors(t *testing.T) []testProcessor {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("failed to create BackupProcessor: %v", err)
	}
	return []testProcessor{
		{
			processor: NewInMemoryBackupProcessor(),
			cleanup:   func() {},
		},
		{
			processor: bp,
			cleanup: func() {
				_ = bp.Close()
			},
		},
	}
}

func TestNewInMemoryBackupProcessor(t *testing.T) {
	bp := NewInMemoryBackupProcessor()
	if bp == nil {
		t.Fatal("NewInMemoryBackupProcessor returned nil")
	}
	if bp.srcFiles == nil {
		t.Fatal("files map not initialized")
	}
	if len(bp.srcFiles) != 0 {
		t.Errorf("expected empty files map, got %d entries", len(bp.srcFiles))
	}
}

func TestNewBackupProcessor(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("NewBackupProcessor failed: %v", err)
	}
	if bp == nil {
		t.Fatal("NewBackupProcessor returned nil")
	}
	defer bp.Close()

	if bp.srcFileCollection == nil {
		t.Fatal("fileCollection not initialized")
	}
	if bp.session == nil {
		t.Fatal("session not initialized")
	}
}

func TestAddFile(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			md5Key := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4" // Valid 32-char hex MD5
			size := int64(1024)
			backupDest := []string{"dest1", "dest2"}
			fpath := core.NewFpath("/path/to/file.txt")

			err := bp.addSrcFile(md5Key, size, backupDest, fpath)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			// Verify by iterating
			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			count := 0
			var fp core.Fpath
			for fd := range iter {
				fp = fd.Fpath
				count++
			}
			if count != 1 {
				t.Fatalf("expected one file from iterator, got %d", count)
			}
			if fp != fpath {
				t.Errorf("expected fpath %s, got %s", fpath, fp)
			}
		})
	}
}

func getProcessorName(i int) string {
	if i == 0 {
		return "inMemoryBackupProcessor"
	}
	return "BackupProcessor"
}

// generateMD5Key generates a valid 32-character hex MD5 key for testing
func generateMD5Key(seed string) string {
	// Create a simple hash from the seed to produce valid hex
	// Convert each char in seed to a byte, then to hex
	var hex string
	for i, ch := range seed {
		hex += fmt.Sprintf("%02x", byte(ch)+byte(i))
	}
	// Pad with zeros if needed
	for len(hex) < 32 {
		hex += "0"
	}
	return hex[:32]
}

func TestAddFileOverwrite(t *testing.T) {
	// Only test on inMemoryBackupProcessor which supports overwriting
	bp := NewInMemoryBackupProcessor()

	md5Key := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"

	// Add first file
	err := bp.addSrcFile(md5Key, 100, []string{"dest1"}, core.NewFpath("/file1.txt"))
	if err != nil {
		t.Fatalf("first addFile failed: %v", err)
	}

	// Add second file with same key (should overwrite)
	err = bp.addSrcFile(md5Key, 200, []string{"dest2", "dest3"}, core.NewFpath("/file2.txt"))
	if err != nil {
		t.Fatalf("second addFile failed: %v", err)
	}

	// Verify the overwritten file is returned
	iter, err := bp.prioritizedSrcFiles(10)
	if err != nil {
		t.Fatalf("prioritizedFiles failed: %v", err)
	}

	count := 0
	var fp core.Fpath
	for fd := range iter {
		fp = fd.Fpath
		count++
	}
	if count != 1 {
		t.Fatalf("expected one file from iterator, got %d", count)
	}
	if fp.String() != "/file1.txt" {
		t.Errorf("expected /file1.txt (fewest backups retained), got %s", fp)
	}
}

func TestPrioritizedFilesEmpty(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// Should immediately return on empty processor
			count := 0
			for range iter {
				count++
			}
			if count != 0 {
				t.Errorf("expected no files on empty processor, got %d", count)
			}
		})
	}
}

func TestPrioritizedFilesSingleFile(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			fpath := core.NewFpath("/single/file.txt")
			err := bp.addSrcFile("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4", 500, []string{"dest1"}, fpath)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			count := 0
			var fp core.Fpath
			for fd := range iter {
				fp = fd.Fpath
				count++
			}
			if count != 1 {
				t.Fatalf("expected one file from iterator, got %d", count)
			}
			if fp != fpath {
				t.Errorf("expected %s, got %s", fpath, fp)
			}
		})
	}
}

func TestPrioritizedFilesOrdering(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			// Add files with different BackupDest lengths (use unique MD5s)
			file0 := core.NewFpath("/file0.txt")
			err := bp.addSrcFile("00000000000000000000000000000000", 100, []string{}, file0)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			file3 := core.NewFpath("/file3.txt")
			err = bp.addSrcFile("33333333333333333333333333333333", 400, []string{"d1", "d2", "d3"}, file3)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			file1 := core.NewFpath("/file1.txt")
			err = bp.addSrcFile("11111111111111111111111111111111", 200, []string{"dest1"}, file1)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			file2 := core.NewFpath("/file2.txt")
			err = bp.addSrcFile("22222222222222222222222222222222", 300, []string{"dest1", "dest2"}, file2)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// Verify order: 0, 1, 2, 3 backups
			expected := []core.Fpath{file0, file1, file2, file3}
			i := 0
			for fd := range iter {
				if i >= len(expected) {
					t.Fatalf("iterator returned more than %d files", len(expected))
				}
				if fd.Fpath != expected[i] {
					t.Errorf("position %d: expected %s, got %s", i, expected[i], fd.Fpath)
				}
				i++
			}
			if i != len(expected) {
				t.Errorf("expected %d files, got %d", len(expected), i)
			}
		})
	}
}

func TestPrioritizedFilesSkipAlreadyBackedUp(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			missingPath := core.NewFpath("/missing.txt")
			backedPath := core.NewFpath("/backed.txt")

			if err := bp.addSrcFile("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 200, []string{"d1"}, missingPath); err != nil {
				t.Fatalf("addSrcFile missing failed: %v", err)
			}
			if err := bp.addSrcFile("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, []string{"d1", "d2"}, backedPath); err != nil {
				t.Fatalf("addSrcFile backed failed: %v", err)
			}
			if err := bp.addDstFile("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, []string{"d1", "d2"}, backedPath); err != nil {
				t.Fatalf("addDstFile failed: %v", err)
			}

			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			count := 0
			var fp core.Fpath
			for fd := range iter {
				fp = fd.Fpath
				count++
			}
			if count != 1 {
				t.Fatalf("expected one file to back up, got %d", count)
			}
			if fp != missingPath {
				t.Fatalf("expected %s, got %s", missingPath, fp)
			}
		})
	}
}

func TestPrioritizedFilesBucketAndSizeOrdering(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			cases := []struct {
				md5   string
				size  int64
				dests []string
				path  core.Fpath
			}{
				{md5: generateMD5Key("a"), size: 300, dests: []string{}, path: core.NewFpath("/len0_big.dat")},
				{md5: generateMD5Key("b"), size: 100, dests: []string{}, path: core.NewFpath("/len0_small.dat")},
				{md5: generateMD5Key("c"), size: 400, dests: []string{"d1"}, path: core.NewFpath("/len1_big.dat")},
				{md5: generateMD5Key("d"), size: 50, dests: []string{"d1"}, path: core.NewFpath("/len1_small.dat")},
			}

			for _, c := range cases {
				if err := bp.addSrcFile(c.md5, c.size, c.dests, c.path); err != nil {
					t.Fatalf("addSrcFile failed for %s: %v", c.path, err)
				}
			}

			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			expected := []core.Fpath{
				core.NewFpath("/len0_big.dat"),
				core.NewFpath("/len0_small.dat"),
				core.NewFpath("/len1_big.dat"),
				core.NewFpath("/len1_small.dat"),
			}

			idx := 0
			for fd := range iter {
				if idx >= len(expected) {
					t.Fatalf("iterator returned more than %d files", len(expected))
				}
				if fd.Fpath != expected[idx] {
					t.Fatalf("position %d: expected %s, got %s", idx, expected[idx], fd.Fpath)
				}
				idx++
			}
			if idx != len(expected) {
				t.Errorf("expected %d files, got %d", len(expected), idx)
			}
		})
	}
}

func TestPrioritizedFilesSameBackupLength(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			file1 := core.NewFpath("/file1.txt")
			file2 := core.NewFpath("/file2.txt")
			file3 := core.NewFpath("/file3.txt")

			err := bp.addSrcFile(generateMD5Key("key1"), 100, []string{"d1", "d2"}, file1)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}
			err = bp.addSrcFile(generateMD5Key("key2"), 200, []string{"d3", "d4"}, file2)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}
			err = bp.addSrcFile(generateMD5Key("key3"), 300, []string{"d5", "d6"}, file3)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			iter, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// All files should be returned (order within same length is not guaranteed)
			seenFiles := make(map[core.Fpath]bool)
			count := 0
			for fd := range iter {
				seenFiles[fd.Fpath] = true
				count++
			}

			if count != 3 {
				t.Errorf("expected 3 files, got %d", count)
			}
			if !seenFiles[file1] || !seenFiles[file2] || !seenFiles[file3] {
				t.Error("not all files were returned by iterator")
			}
		})
	}
}

func TestPrioritizedFilesMultipleCalls(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			file1 := core.NewFpath("/file1.txt")
			err := bp.addSrcFile(generateMD5Key("key1"), 100, []string{}, file1)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			// First call to prioritizedFiles
			iter1, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("first prioritizedFiles failed: %v", err)
			}

			count1 := 0
			var fp1 core.Fpath
			for fd := range iter1 {
				fp1 = fd.Fpath
				count1++
			}
			if count1 != 1 || fp1 != file1 {
				t.Error("first iterator failed to return file")
			}

			// Second call should create a new iterator
			iter2, err := bp.prioritizedSrcFiles(10)
			if err != nil {
				t.Fatalf("second prioritizedFiles failed: %v", err)
			}

			count2 := 0
			var fp2 core.Fpath
			for fd := range iter2 {
				fp2 = fd.Fpath
				count2++
			}
			if count2 != 1 || fp2 != file1 {
				t.Error("second iterator failed to return file")
			}
		})
	}
}

func TestPrioritizedFilesWithManyFiles(t *testing.T) {
	// Only test on inMemoryBackupProcessor which supports deduplication
	bp := NewInMemoryBackupProcessor()

	// Add 100 files with varying backup destinations
	for i := 0; i < 100; i++ {
		md5Key := generateMD5Key(string(rune('a' + (i % 26))))
		backupCount := i % 5 // 0-4 backups
		backupDest := make([]string, backupCount)
		for j := 0; j < backupCount; j++ {
			backupDest[j] = string(rune('x' + j))
		}
		fpath := core.NewFpath(fmt.Sprintf("/file_%03d", i))
		err := bp.addSrcFile(md5Key, int64(i*100), backupDest, fpath)
		if err != nil {
			t.Fatalf("addFile failed for file %d: %v", i, err)
		}
	}

	// Capture the processor's final view of src files (after merging logic)
	pathCounts := make(map[core.Fpath]int)
	for _, fd := range bp.srcFiles {
		pathCounts[fd.Fpath] = len(fd.BackupDest)
	}

	iter, err := bp.prioritizedSrcFiles(10)
	if err != nil {
		t.Fatalf("prioritizedFiles failed: %v", err)
	}

	// Verify we get files in order of increasing backup destination count
	prevBackupCount := -1
	count := 0
	for fd := range iter {
		count++

		currentBackupCount := len(fd.BackupDest)

		if currentBackupCount < prevBackupCount {
			t.Errorf("files not in order: got backup count %d after %d", currentBackupCount, prevBackupCount)
		}
		prevBackupCount = currentBackupCount
	}

	// Expect exactly the entries present in the processor's src map
	if count != len(pathCounts) {
		t.Errorf("expected %d files (unique keys), got %d", len(pathCounts), count)
	}
}
