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
	prioritizedSrcFiles() (func() (core.Fpath, bool), error)
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
			fpath := core.Fpath("/path/to/file.txt")

			err := bp.addSrcFile(md5Key, size, backupDest, fpath)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			// Verify by iterating
			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			fp, ok := next()
			if !ok {
				t.Fatal("expected one file from iterator")
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
	err := bp.addSrcFile(md5Key, 100, []string{"dest1"}, core.Fpath("/file1.txt"))
	if err != nil {
		t.Fatalf("first addFile failed: %v", err)
	}

	// Add second file with same key (should overwrite)
	err = bp.addSrcFile(md5Key, 200, []string{"dest2", "dest3"}, core.Fpath("/file2.txt"))
	if err != nil {
		t.Fatalf("second addFile failed: %v", err)
	}

	// Verify the overwritten file is returned
	next, err := bp.prioritizedSrcFiles()
	if err != nil {
		t.Fatalf("prioritizedFiles failed: %v", err)
	}

	fp, ok := next()
	if !ok {
		t.Fatal("expected one file from iterator")
	}
	if string(fp) != "/file2.txt" {
		t.Errorf("expected /file2.txt after overwrite, got %s", fp)
	}
}

func TestPrioritizedFilesEmpty(t *testing.T) {
	processors := getTestProcessors(t)

	for i, tp := range processors {
		t.Run(getProcessorName(i), func(t *testing.T) {
			defer tp.cleanup()
			bp := tp.processor

			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// Should immediately return false on empty processor
			_, ok := next()
			if ok {
				t.Error("expected iterator to be exhausted on empty processor")
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

			fpath := core.Fpath("/single/file.txt")
			err := bp.addSrcFile("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4", 500, []string{"dest1"}, fpath)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			fp, ok := next()
			if !ok {
				t.Fatal("expected one file from iterator")
			}
			if fp != fpath {
				t.Errorf("expected %s, got %s", fpath, fp)
			}

			// Should be exhausted now
			_, ok = next()
			if ok {
				t.Error("expected iterator to be exhausted after one file")
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
			file0 := core.Fpath("/file0.txt")
			err := bp.addSrcFile("00000000000000000000000000000000", 100, []string{}, file0)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			file3 := core.Fpath("/file3.txt")
			err = bp.addSrcFile("33333333333333333333333333333333", 400, []string{"d1", "d2", "d3"}, file3)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			file1 := core.Fpath("/file1.txt")
			err = bp.addSrcFile("11111111111111111111111111111111", 200, []string{"dest1"}, file1)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			file2 := core.Fpath("/file2.txt")
			err = bp.addSrcFile("22222222222222222222222222222222", 300, []string{"dest1", "dest2"}, file2)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// Verify order: 0, 1, 2, 3 backups
			expected := []core.Fpath{file0, file1, file2, file3}
			for i, expectedPath := range expected {
				fp, ok := next()
				if !ok {
					t.Fatalf("iterator exhausted at position %d, expected 4 files", i)
				}
				if fp != expectedPath {
					t.Errorf("position %d: expected %s, got %s", i, expectedPath, fp)
				}
			}

			// Should be exhausted now
			_, ok := next()
			if ok {
				t.Error("expected iterator to be exhausted after 4 files")
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

			missingPath := core.Fpath("/missing.txt")
			backedPath := core.Fpath("/backed.txt")

			if err := bp.addSrcFile("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 200, []string{"d1"}, missingPath); err != nil {
				t.Fatalf("addSrcFile missing failed: %v", err)
			}
			if err := bp.addSrcFile("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, []string{"d1", "d2"}, backedPath); err != nil {
				t.Fatalf("addSrcFile backed failed: %v", err)
			}
			if err := bp.addDstFile("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, []string{"d1", "d2"}, backedPath); err != nil {
				t.Fatalf("addDstFile failed: %v", err)
			}

			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			fp, ok := next()
			if !ok {
				t.Fatal("expected one file to back up")
			}
			if fp != missingPath {
				t.Fatalf("expected %s, got %s", missingPath, fp)
			}

			if _, ok := next(); ok {
				t.Fatal("expected iterator exhaustion after missing file")
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
				{md5: generateMD5Key("a"), size: 300, dests: []string{}, path: core.Fpath("/len0_big.dat")},
				{md5: generateMD5Key("b"), size: 100, dests: []string{}, path: core.Fpath("/len0_small.dat")},
				{md5: generateMD5Key("c"), size: 400, dests: []string{"d1"}, path: core.Fpath("/len1_big.dat")},
				{md5: generateMD5Key("d"), size: 50, dests: []string{"d1"}, path: core.Fpath("/len1_small.dat")},
			}

			for _, c := range cases {
				if err := bp.addSrcFile(c.md5, c.size, c.dests, c.path); err != nil {
					t.Fatalf("addSrcFile failed for %s: %v", c.path, err)
				}
			}

			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			expected := []core.Fpath{
				core.Fpath("/len0_big.dat"),
				core.Fpath("/len0_small.dat"),
				core.Fpath("/len1_big.dat"),
				core.Fpath("/len1_small.dat"),
			}

			for idx, want := range expected {
				fp, ok := next()
				if !ok {
					t.Fatalf("iterator exhausted at %d", idx)
				}
				if fp != want {
					t.Fatalf("position %d: expected %s, got %s", idx, want, fp)
				}
			}

			if _, ok := next(); ok {
				t.Fatal("expected iterator to be exhausted after all files")
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

			file1 := core.Fpath("/file1.txt")
			file2 := core.Fpath("/file2.txt")
			file3 := core.Fpath("/file3.txt")

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

			next, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// All files should be returned (order within same length is not guaranteed)
			seenFiles := make(map[core.Fpath]bool)
			for i := range 3 {
				fp, ok := next()
				if !ok {
					t.Fatalf("iterator exhausted at position %d, expected 3 files", i)
				}
				seenFiles[fp] = true
			}

			if !seenFiles[file1] || !seenFiles[file2] || !seenFiles[file3] {
				t.Error("not all files were returned by iterator")
			}

			_, ok := next()
			if ok {
				t.Error("expected iterator to be exhausted after 3 files")
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

			file1 := core.Fpath("/file1.txt")
			err := bp.addSrcFile(generateMD5Key("key1"), 100, []string{}, file1)
			if err != nil {
				t.Fatalf("addFile failed: %v", err)
			}

			// First call to prioritizedFiles
			next1, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("first prioritizedFiles failed: %v", err)
			}

			fp1, ok := next1()
			if !ok || fp1 != file1 {
				t.Error("first iterator failed to return file")
			}

			// Second call should create a new iterator
			next2, err := bp.prioritizedSrcFiles()
			if err != nil {
				t.Fatalf("second prioritizedFiles failed: %v", err)
			}

			fp2, ok := next2()
			if !ok || fp2 != file1 {
				t.Error("second iterator failed to return file")
			}
		})
	}
}

func TestPrioritizedFilesWithManyFiles(t *testing.T) {
	// Only test on inMemoryBackupProcessor which supports deduplication
	bp := NewInMemoryBackupProcessor()

	// Track files added for verification
	type fileInfo struct {
		fpath       core.Fpath
		backupCount int
	}
	addedFiles := make(map[string]fileInfo)

	// Add 100 files with varying backup destinations
	for i := 0; i < 100; i++ {
		md5Key := generateMD5Key(string(rune('a' + (i % 26))))
		backupCount := i % 5 // 0-4 backups
		backupDest := make([]string, backupCount)
		for j := 0; j < backupCount; j++ {
			backupDest[j] = string(rune('x' + j))
		}
		fpath := core.Fpath(string(rune('/' + i)))
		err := bp.addSrcFile(md5Key, int64(i*100), backupDest, fpath)
		if err != nil {
			t.Fatalf("addFile failed for file %d: %v", i, err)
		}
		// Track last added file with this key (will overwrite in inMemory)
		addedFiles[md5Key] = fileInfo{fpath: fpath, backupCount: backupCount}
	}

	next, err := bp.prioritizedSrcFiles()
	if err != nil {
		t.Fatalf("prioritizedFiles failed: %v", err)
	}

	// Verify we get files in order of increasing backup destination count
	prevBackupCount := -1
	count := 0
	for {
		fp, ok := next()
		if !ok {
			break
		}
		count++

		// Find the backup count for this file
		var currentBackupCount int
		found := false
		for _, info := range addedFiles {
			if info.fpath == fp {
				currentBackupCount = info.backupCount
				found = true
				break
			}
		}

		if !found {
			t.Errorf("returned file %s not in added files", fp)
			continue
		}

		if currentBackupCount < prevBackupCount {
			t.Errorf("files not in order: got backup count %d after %d", currentBackupCount, prevBackupCount)
		}
		prevBackupCount = currentBackupCount
	}

	// We added 100 files but with duplicate keys (26 possible keys)
	// So we should have exactly 26 unique files (only last one for each key is kept)
	if count != 26 {
		t.Errorf("expected 26 files (unique keys), got %d", count)
	}
}
