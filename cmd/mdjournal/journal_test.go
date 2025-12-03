package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Unit tests for journal creation logic

// testCase defines a test scenario for mdjournal
type testCase struct {
	name          string
	setupFiles    func(string) error       // Create test files/directories
	validateFunc  func(*testing.T, string) // Validate journal contents
	expectedDirs  int                      // Expected number of directories with files
	expectedFiles int                      // Expected number of total files
}

// setupTestDir creates a temp directory and journal
func setupTestDir(t *testing.T, tc testCase) (tempDir, journalPath string, journal consumers.Journal) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "mdjournal-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	if tc.setupFiles != nil {
		if err := tc.setupFiles(tempDir); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("Failed to setup test files: %v", err)
		}
	}

	journal, err = createJournalForDirectories([]string{tempDir})
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create journal: %v", err)
	}

	journalPath = filepath.Join(tempDir, ".mdjournal.xml")
	return
}

// writeAndReadJournal writes journal to file and reads it back
func writeAndReadJournal(t *testing.T, journal consumers.Journal, journalPath string) consumers.Journal {
	t.Helper()
	fh, err := os.Create(journalPath)
	if err != nil {
		t.Fatalf("Failed to create journal file: %v", err)
	}
	err = journal.ToWriter(fh)
	fh.Close()
	if err != nil {
		t.Fatalf("Failed to write journal: %v", err)
	}

	fhRead, err := os.Open(journalPath)
	if err != nil {
		t.Fatalf("Failed to open journal file: %v", err)
	}
	defer fhRead.Close()

	journalRead := consumers.Journal{}
	if err = journalRead.FromReader(fhRead); err != nil {
		t.Fatalf("Failed to read journal: %v", err)
	}
	return journalRead
}

// countJournalContents counts directories and files in journal
func countJournalContents(t *testing.T, journal consumers.Journal) (dirCount, fileCount int, files map[string]bool) {
	t.Helper()
	files = make(map[string]bool)
	err := journal.Range(func(de core.DirectoryEntryJournalableInterface, dir string) error {
		if de.Len() > 0 {
			dirCount++
		}
		de.Revisit(dir, func(dm core.DirectoryEntryInterface, directory string, file string, fs core.FileStruct) error {
			fileCount++
			files[fs.Name] = true
			return nil
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Error ranging over journal: %v", err)
	}
	return
}

func TestJournalCreation(t *testing.T) {
	tests := []testCase{
		{
			name:          "EmptyDirectory",
			setupFiles:    nil, // No files
			expectedDirs:  0,
			expectedFiles: 0,
		},
		{
			name: "SingleFile",
			setupFiles: func(dir string) error {
				return os.WriteFile(filepath.Join(dir, "test.txt"), []byte("content"), 0o644)
			},
			expectedDirs:  1,
			expectedFiles: 1,
		},
		{
			name: "MultipleFiles",
			setupFiles: func(dir string) error {
				for _, name := range []string{"file1.txt", "file2.txt", "file3.dat"} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte("content"), 0o644); err != nil {
						return err
					}
				}
				return nil
			},
			expectedDirs:  1,
			expectedFiles: 3,
		},
		{
			name: "NestedDirectories",
			setupFiles: func(dir string) error {
				subDir1 := filepath.Join(dir, "subdir1")
				subDir2 := filepath.Join(subDir1, "subdir2")
				if err := os.MkdirAll(subDir2, 0o755); err != nil {
					return err
				}
				files := map[string]string{
					filepath.Join(dir, "root.txt"):       "root",
					filepath.Join(subDir1, "level1.txt"): "level1",
					filepath.Join(subDir2, "level2.txt"): "level2",
				}
				for path, content := range files {
					if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
						return err
					}
				}
				return nil
			},
			expectedDirs:  3,
			expectedFiles: 3,
		},
		{
			name: "HiddenFilesExcluded",
			setupFiles: func(dir string) error {
				os.WriteFile(filepath.Join(dir, "visible.txt"), []byte("visible"), 0o644)
				os.WriteFile(filepath.Join(dir, ".hidden.txt"), []byte("hidden"), 0o644)
				hiddenDir := filepath.Join(dir, ".hiddendir")
				os.Mkdir(hiddenDir, 0o755)
				return os.WriteFile(filepath.Join(hiddenDir, "inside.txt"), []byte("inside"), 0o644)
			},
			validateFunc: func(t *testing.T, journalPath string) {
				fhRead, _ := os.Open(journalPath)
				defer fhRead.Close()
				journal := consumers.Journal{}
				journal.FromReader(fhRead)

				_, _, files := countJournalContents(t, journal)
				if !files["visible.txt"] {
					t.Error("Visible file should be in journal")
				}
				if files[".hidden.txt"] {
					t.Error("Hidden file should not be in journal")
				}
				if files["inside.txt"] {
					t.Error("File in hidden directory should not be in journal")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tempDir, journalPath, journal := setupTestDir(t, tc)
			defer os.RemoveAll(tempDir)

			journalRead := writeAndReadJournal(t, journal, journalPath)

			if tc.validateFunc != nil {
				tc.validateFunc(t, journalPath)
			} else {
				dirCount, fileCount, _ := countJournalContents(t, journalRead)
				if dirCount != tc.expectedDirs {
					t.Errorf("Expected %d directories, got %d", tc.expectedDirs, dirCount)
				}
				if fileCount != tc.expectedFiles {
					t.Errorf("Expected %d files, got %d", tc.expectedFiles, fileCount)
				}
			}
		})
	}
}

func TestMultipleRootDirectories(t *testing.T) {
	tempDir1, _ := os.MkdirTemp("", "mdjournal-test-*")
	defer os.RemoveAll(tempDir1)
	tempDir2, _ := os.MkdirTemp("", "mdjournal-test-*")
	defer os.RemoveAll(tempDir2)

	os.WriteFile(filepath.Join(tempDir1, "file1.txt"), []byte("content1"), 0o644)
	os.WriteFile(filepath.Join(tempDir2, "file2.txt"), []byte("content2"), 0o644)

	journal, err := createJournalForDirectories([]string{tempDir1, tempDir2})
	if err != nil {
		t.Fatalf("Failed to create journal: %v", err)
	}

	journalPath := filepath.Join(tempDir1, ".mdjournal.xml")
	journalRead := writeAndReadJournal(t, journal, journalPath)

	_, _, files := countJournalContents(t, journalRead)
	if !files["file1.txt"] || !files["file2.txt"] {
		t.Error("Files from both directories should be in journal")
	}
}

func TestJournalPersistence(t *testing.T) {
	tempDir, journalPath, journal := setupTestDir(t, testCase{
		setupFiles: func(dir string) error {
			for _, name := range []string{"alpha.txt", "beta.dat", "gamma.log"} {
				os.WriteFile(filepath.Join(dir, name), []byte("content"), 0o644)
			}
			return nil
		},
	})
	defer os.RemoveAll(tempDir)

	journalRead := writeAndReadJournal(t, journal, journalPath)
	if err := journal.Equals(journalRead, nil); err != nil {
		t.Errorf("Journals not equal after persistence: %v", err)
	}
}

// createJournalForDirectories is a helper function for testing
// This extracts the core journaling logic
func createJournalForDirectories(directories []string) (consumers.Journal, error) {
	journal := consumers.Journal{}
	// Store directory maps to add to journal after processing
	dirMaps := make(map[string]*core.DirectoryMap)

	visitor := func(dm core.DirectoryMap, directory, file string, d os.DirEntry) error {
		// Skip the md5 file itself
		if file == core.Md5FileName {
			return nil
		}

		// Skip hidden files (files starting with .)
		if len(file) > 0 && file[0] == '.' {
			return nil
		}

		// Update the directory map with this file's information
		fc := func(fs *core.FileStruct) error {
			info, err := d.Info()
			if err != nil {
				return err
			}
			_, err = fs.FromStat(directory, file, info)
			if err != nil {
				return err
			}
			// Update checksum for the file
			return fs.UpdateChecksum(false)
		}
		return dm.RunFsFc(directory, file, fc)
	}

	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			if err != nil {
				return &dm, err
			}
			dm.VisitFunc = visitor
			// Store the directory map pointer so we can add to journal later
			dirMaps[dir] = &dm

			return &dm, nil
		}
		de, err := core.NewDirectoryEntry(dir, mkFk)
		return de, err
	}

	for _, dir := range directories {
		errChan := core.NewDirTracker(false, dir, makerFunc).ErrChan()
		for err := range errChan {
			return journal, err
		}
	}

	// Now add all the processed directory maps to the journal
	for dir, dm := range dirMaps {
		if err := journal.AppendJournalFromDm(dm, dir); err != nil {
			// ErrFileExistsInJournal is not a real error, just informational
			if !errors.Is(err, consumers.ErrFileExistsInJournal) {
				return journal, err
			}
		}
	}

	return journal, nil
}
