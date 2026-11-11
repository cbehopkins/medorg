package consumers

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
)

// backupProcessorInterface defines common interface for both implementations
type backupProcessorInterface interface {
	addSrcFile(md5Key string, size int64, backupDest []string, file core.Fpath) error
	addDstFile(md5Key string, size int64, backupDest []string, file core.Fpath) error
	prioritizedSrcFiles(maxNumBackups int, shutdown <-chan struct{}) (func(yield func(fileData) bool) error, error)
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

func collectBackupFiles(t *testing.T, iterator func(func(fileData) bool) error) []fileData {
	t.Helper()
	files := make([]fileData, 0)
	err := iterator(func(file fileData) bool {
		files = append(files, file)
		return true
	})
	if err != nil {
		t.Fatalf("failed to iterate backup files: %v", err)
	}
	return files
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

	if err := bp.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
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
			iter, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			count := 0
			var fp core.Fpath
			for _, fd := range collectBackupFiles(t, iter) {
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
	iter, err := bp.prioritizedSrcFiles(10, nil)
	if err != nil {
		t.Fatalf("prioritizedFiles failed: %v", err)
	}

	count := 0
	var fp core.Fpath
	for _, fd := range collectBackupFiles(t, iter) {
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

			iter, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// Should immediately return on empty processor
			count := 0
			for range collectBackupFiles(t, iter) {
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

			iter, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			count := 0
			var fp core.Fpath
			for _, fd := range collectBackupFiles(t, iter) {
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

			iter, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// Verify order: 0, 1, 2, 3 backups
			expected := []core.Fpath{file0, file1, file2, file3}
			i := 0
			for _, fd := range collectBackupFiles(t, iter) {
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

			iter, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			count := 0
			var fp core.Fpath
			for _, fd := range collectBackupFiles(t, iter) {
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

			iter, err := bp.prioritizedSrcFiles(10, nil)
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
			for _, fd := range collectBackupFiles(t, iter) {
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

			iter, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("prioritizedFiles failed: %v", err)
			}

			// All files should be returned (order within same length is not guaranteed)
			seenFiles := make(map[core.Fpath]bool)
			count := 0
			for _, fd := range collectBackupFiles(t, iter) {
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
			iter1, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("first prioritizedFiles failed: %v", err)
			}

			count1 := 0
			var fp1 core.Fpath
			for _, fd := range collectBackupFiles(t, iter1) {
				fp1 = fd.Fpath
				count1++
			}
			if count1 != 1 || fp1 != file1 {
				t.Error("first iterator failed to return file")
			}

			// Second call should create a new iterator
			iter2, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatalf("second prioritizedFiles failed: %v", err)
			}

			count2 := 0
			var fp2 core.Fpath
			for _, fd := range collectBackupFiles(t, iter2) {
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

	iter, err := bp.prioritizedSrcFiles(10, nil)
	if err != nil {
		t.Fatalf("prioritizedFiles failed: %v", err)
	}

	// Verify we get files in order of increasing backup destination count
	prevBackupCount := -1
	count := 0
	for _, fd := range collectBackupFiles(t, iter) {
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

func TestBackupProcessorBatchedInventoryParity(t *testing.T) {
	for _, batchSize := range []int{1, 3} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			bp, err := newBackupProcessor(batchSize)
			if err != nil {
				t.Fatal(err)
			}
			defer bp.Close()

			const (
				destinationOnly = "cccccccccccccccccccccccccccccccc"
				sharedContent   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				uniqueContent   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			)
			if err := bp.addDstFile(destinationOnly, 10, []string{"volume-dst"}, core.NewFpath("/dst/orphan")); err != nil {
				t.Fatal(err)
			}
			if err := bp.addSrcFile(sharedContent, 20, []string{"volume-1", "volume-2"}, core.NewFpath("/src/older-copy")); err != nil {
				t.Fatal(err)
			}
			if err := bp.addSrcFile(sharedContent, 20, []string{"volume-2"}, core.NewFpath("/src/preferred-copy")); err != nil {
				t.Fatal(err)
			}
			if err := bp.addSrcFile(uniqueContent, 30, nil, core.NewFpath("/src/unique")); err != nil {
				t.Fatal(err)
			}

			if _, exists, err := bp.checkDstFileExists(destinationOnly); err != nil || !exists {
				t.Fatalf("destination lookup = %v, %v; want exists", exists, err)
			}

			orphans, err := bp.getOrphanFiles(nil)
			if err != nil {
				t.Fatal(err)
			}
			var orphanPaths []string
			if err := orphans(func(path core.Fpath) bool {
				orphanPaths = append(orphanPaths, path.String())
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if len(orphanPaths) != 1 || orphanPaths[0] != "/dst/orphan" {
				t.Fatalf("orphan paths = %v, want [/dst/orphan]", orphanPaths)
			}

			iterator, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatal(err)
			}
			files := collectBackupFiles(t, iterator)
			if len(files) != 2 {
				t.Fatalf("planned files = %d, want 2", len(files))
			}
			if got := files[0].Fpath.String(); got != "/src/unique" {
				t.Errorf("first planned path = %q, want /src/unique", got)
			}
			if got := files[1].Fpath.String(); got != "/src/preferred-copy" {
				t.Errorf("deduplicated path = %q, want /src/preferred-copy", got)
			}
			if got := fmt.Sprint(files[1].BackupDest); got != "[volume-1 volume-2]" {
				t.Errorf("merged destinations = %s, want [volume-1 volume-2]", got)
			}
		})
	}
}

func TestBackupProcessorBufferedInventory(t *testing.T) {
	for _, batchSize := range []int{1, 3, 128} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			bp, err := newBackupProcessor(batchSize)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := bp.Close(); err != nil {
					t.Error(err)
				}
			})
			const checksum = "abcdef0123456789abcdef0123456789"
			decoded, err := hex.DecodeString(checksum)
			if err != nil {
				t.Fatal(err)
			}
			labels := []string{"volume-1", "volume-2"}
			if err := bp.addSrcFile(strings.ToUpper(checksum), 20, labels, core.NewFpath("/src/first")); err != nil {
				t.Fatal(err)
			}
			labels[0] = "mutated"
			if err := bp.addSrcFile(base64.RawStdEncoding.EncodeToString(decoded), 30, []string{"volume-2"}, core.NewFpath("/src/preferred")); err != nil {
				t.Fatal(err)
			}
			if err := bp.addSrcFile(checksum, 40, []string{"volume-3", "volume-4"}, core.NewFpath("/src/later")); err != nil {
				t.Fatal(err)
			}
			iterator, err := bp.prioritizedSrcFiles(10, nil)
			if err != nil {
				t.Fatal(err)
			}
			files := collectBackupFiles(t, iterator)
			if len(files) != 1 {
				t.Fatalf("source files = %d, want 1", len(files))
			}
			if files[0].Fpath.String() != "/src/preferred" || files[0].Size != 30 {
				t.Errorf("source representative = %s (%d), want /src/preferred (30)", files[0].Fpath, files[0].Size)
			}
			if !slices.Equal(files[0].BackupDest, []string{"volume-1", "volume-2", "volume-3", "volume-4"}) {
				t.Errorf("source destinations = %v", files[0].BackupDest)
			}
			for index, destinations := range [][]string{{"volume-1", "volume-2"}, {"volume-2"}, {"volume-3"}, {"volume-4"}} {
				if err := bp.addDstFile(checksum, int64(index+1), destinations, core.NewFpath(fmt.Sprintf("/dst/copy-%d", index))); err != nil {
					t.Fatal(err)
				}
				path, exists, err := bp.checkDstFileExists(strings.ToUpper(checksum))
				if err != nil || !exists || path.String() != "/dst/copy-0" {
					t.Fatalf("destination lookup = %s, %v, %v; want /dst/copy-0, true, nil", path, exists, err)
				}
			}
			if files := collectBackupFiles(t, iterator); len(files) != 0 {
				t.Fatalf("planned files = %d, want 0 after destination ingestion", len(files))
			}
			var size int64
			var count int
			var destinations string
			if err := bp.db.QueryRow("SELECT size, backup_count, backup_destinations FROM destination_inventory WHERE checksum = ?", checksum).Scan(&size, &count, &destinations); err != nil {
				t.Fatal(err)
			}
			if size != 1 || count != 4 || destinations != `["volume-1","volume-2","volume-3","volume-4"]` {
				t.Errorf("destination record = %d, %d, %s; want first size and merged destinations", size, count, destinations)
			}
			if err := bp.addSrcFile("invalid", 1, nil, core.NewFpath("/src/invalid")); err == nil {
				t.Error("invalid checksum accepted")
			}
		})
	}
}

func TestBackupProcessorDestinationLookupKeepsSourceBatch(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bp.Close(); err != nil {
			t.Error(err)
		}
	})
	const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := bp.addSrcFile(checksum, 10, nil, core.NewFpath("/src/file")); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := bp.checkDstFileExists(checksum); err != nil || exists {
		t.Fatalf("destination lookup = %v, %v; want false, nil", exists, err)
	}
	if len(bp.pendingSrc) != 1 {
		t.Fatal("destination lookup flushed the source batch")
	}
}

func TestBackupProcessorConcurrentDuplicateSubmissions(t *testing.T) {
	bp, err := newBackupProcessor(3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bp.Close(); err != nil {
			t.Error(err)
		}
	})
	const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const submissionCount = 40
	var workers sync.WaitGroup
	for index := 0; index < submissionCount; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			if err := bp.addSrcFile(checksum, 10, []string{fmt.Sprintf("volume-%d", index)}, core.NewFpath(fmt.Sprintf("/src/copy-%d", index))); err != nil {
				t.Error(err)
			}
		}(index)
	}
	workers.Wait()
	iterator, err := bp.prioritizedSrcFiles(submissionCount, nil)
	if err != nil {
		t.Fatal(err)
	}
	files := collectBackupFiles(t, iterator)
	if len(files) != 1 {
		t.Fatalf("source files = %d, want 1", len(files))
	}
	if len(files[0].BackupDest) != submissionCount {
		t.Fatalf("merged destinations = %d, want %d", len(files[0].BackupDest), submissionCount)
	}
	for index := 0; index < submissionCount; index++ {
		if !slices.Contains(files[0].BackupDest, fmt.Sprintf("volume-%d", index)) {
			t.Errorf("missing destination volume-%d", index)
		}
	}
}

func TestBackupProcessorTemporaryInventoryDurability(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bp.Close(); err != nil {
			t.Error(err)
		}
	})
	var synchronous int
	if err := bp.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 0 {
		t.Errorf("synchronous = %d, want OFF (0) for temporary inventory", synchronous)
	}
	var journalMode string
	if err := bp.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "delete" {
		t.Errorf("journal_mode = %q, want unchanged DELETE journalling", journalMode)
	}
}

func TestBackupProcessorBatchRollbackAndRetry(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bp.Close(); err != nil {
			t.Error(err)
		}
	})
	const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := bp.addSrcFile(checksum, 10, nil, core.NewFpath("/src/file")); err != nil {
		t.Fatal(err)
	}
	if err := bp.addDstFile(checksum, 10, nil, core.NewFpath("/dst/file")); err != nil {
		t.Fatal(err)
	}
	if _, err := bp.db.Exec(`CREATE TRIGGER reject_destination BEFORE INSERT ON destination_inventory
		BEGIN SELECT RAISE(ABORT, 'injected batch failure'); END`); err != nil {
		t.Fatal(err)
	}
	iterator, err := bp.prioritizedSrcFiles(2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := iterator(func(fileData) bool { return true }); err == nil || !strings.Contains(err.Error(), "injected batch failure") {
		t.Fatalf("flush error = %v, want injected batch failure", err)
	}
	for _, table := range []string{"source_inventory", "destination_inventory"} {
		var count int
		if err := bp.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("%s contains %d rows after rollback, want 0", table, count)
		}
	}
	if _, err := bp.db.Exec("DROP TRIGGER reject_destination"); err != nil {
		t.Fatal(err)
	}
	if files := collectBackupFiles(t, iterator); len(files) != 0 {
		t.Errorf("planned files = %d, want 0 after retrying matching source and destination", len(files))
	}
	for _, table := range []string{"source_inventory", "destination_inventory"} {
		var count int
		if err := bp.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("%s contains %d rows after retry, want 1", table, count)
		}
	}
}

func BenchmarkBackupInventoryWriteBatching(b *testing.B) {
	const (
		fileCount       = 4096
		directoryCount  = 64
		uniqueChecksums = 1024
	)
	for _, benchmark := range []struct {
		name      string
		batchSize int
	}{
		{name: "single_record_commit", batchSize: 1},
		{name: "batch_128", batchSize: backupWriteBatchSize},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.Logf("files=%d directories=%d duplicate_checksums=%d files_per_checksum=4 temp_root=%s cache_size_kib=2048 batch_size=%d", fileCount, directoryCount, fileCount-uniqueChecksums, os.TempDir(), benchmark.batchSize)
			b.ResetTimer()
			start := time.Now()
			for iteration := 0; iteration < b.N; iteration++ {
				bp, err := newBackupProcessor(benchmark.batchSize)
				if err != nil {
					b.Fatal(err)
				}
				for fileIndex := 0; fileIndex < fileCount; fileIndex++ {
					checksum := fmt.Sprintf("%032x", fileIndex%uniqueChecksums)
					path := core.NewFpath(
						fmt.Sprintf("/src/dir-%02d", fileIndex%directoryCount),
						fmt.Sprintf("file-%05d.dat", fileIndex),
					)
					if err := bp.addSrcFile(checksum, int64(fileIndex+1), []string{"volume-1"}, path); err != nil {
						_ = bp.Close()
						b.Fatal(err)
					}
				}
				if err := bp.Close(); err != nil {
					b.Fatal(err)
				}
			}
			elapsed := time.Since(start)
			elapsedSeconds := elapsed.Seconds()
			b.ReportMetric(float64(fileCount*b.N)/elapsedSeconds, "files/s")
			b.ReportMetric(float64(directoryCount*b.N)/elapsedSeconds, "directories/s")
			b.Logf("elapsed=%s iterations=%d", elapsed, b.N)
		})
	}
}
