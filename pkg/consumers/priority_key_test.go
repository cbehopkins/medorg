package consumers

import (
	"sort"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestPriorityKeyOrdering tests that the priority key comparator produces the expected ordering:
// - Files with fewer destinations come first (destCount asc)
// - For same destCount, larger files come first (invSize desc, meaning smaller invSize)
// - For same destCount and size, lexicographic path ordering (path asc)
func TestPriorityKeyOrdering(t *testing.T) {
	tests := []struct {
		name     string
		files    []fileData
		expected []string // expected order by path
	}{
		{
			name: "single file",
			files: []fileData{
				{Size: 1000, Fpath: core.Fpath("/file.txt"), BackupDest: []string{"dest1"}},
			},
			expected: []string{"/file.txt"},
		},
		{
			name: "fewest destinations first",
			files: []fileData{
				{Size: 1000, Fpath: core.Fpath("/a.txt"), BackupDest: []string{"dest1", "dest2"}},
				{Size: 1000, Fpath: core.Fpath("/b.txt"), BackupDest: []string{"dest1"}},
				{Size: 1000, Fpath: core.Fpath("/c.txt"), BackupDest: []string{"dest1", "dest2", "dest3"}},
			},
			expected: []string{"/b.txt", "/a.txt", "/c.txt"},
		},
		{
			name: "larger files first when same destCount",
			files: []fileData{
				{Size: 100, Fpath: core.Fpath("/small.txt"), BackupDest: []string{"dest1"}},
				{Size: 5000, Fpath: core.Fpath("/large.txt"), BackupDest: []string{"dest1"}},
				{Size: 1000, Fpath: core.Fpath("/medium.txt"), BackupDest: []string{"dest1"}},
			},
			expected: []string{"/large.txt", "/medium.txt", "/small.txt"},
		},
		{
			name: "path ordering when destCount and size equal",
			files: []fileData{
				{Size: 1000, Fpath: core.Fpath("/zebra.txt"), BackupDest: []string{"dest1"}},
				{Size: 1000, Fpath: core.Fpath("/apple.txt"), BackupDest: []string{"dest1"}},
				{Size: 1000, Fpath: core.Fpath("/monkey.txt"), BackupDest: []string{"dest1"}},
			},
			expected: []string{"/apple.txt", "/monkey.txt", "/zebra.txt"},
		},
		{
			name: "comprehensive ordering test",
			files: []fileData{
				// destCount=2, size=500
				{Size: 500, Fpath: core.Fpath("/z_two_small.txt"), BackupDest: []string{"d1", "d2"}},
				// destCount=1, size=100
				{Size: 100, Fpath: core.Fpath("/z_one_tiny.txt"), BackupDest: []string{"d1"}},
				// destCount=3, size=5000
				{Size: 5000, Fpath: core.Fpath("/a_three_large.txt"), BackupDest: []string{"d1", "d2", "d3"}},
				// destCount=1, size=5000
				{Size: 5000, Fpath: core.Fpath("/a_one_large.txt"), BackupDest: []string{"d1"}},
				// destCount=1, size=100
				{Size: 100, Fpath: core.Fpath("/a_one_tiny.txt"), BackupDest: []string{"d1"}},
				// destCount=2, size=5000
				{Size: 5000, Fpath: core.Fpath("/m_two_large.txt"), BackupDest: []string{"d1", "d2"}},
			},
			expected: []string{
				"/a_one_large.txt",   // destCount=1, size=5000 (larger size first)
				"/a_one_tiny.txt",    // destCount=1, size=100 (path /a < /z)
				"/z_one_tiny.txt",    // destCount=1, size=100 (path /z > /a)
				"/m_two_large.txt",   // destCount=2, size=5000
				"/z_two_small.txt",   // destCount=2, size=500
				"/a_three_large.txt", // destCount=3, size=5000
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert fileData to priorityKey
			keys := make([]priorityKey, len(tt.files))
			for i, fd := range tt.files {
				keys[i] = buildPriorityKey(fd)
			}

			// Sort using the comparator
			sort.Slice(keys, func(i, j int) bool {
				return priorityKeyLess(keys[i], keys[j])
			})

			// Verify order
			if len(keys) != len(tt.expected) {
				t.Fatalf("expected %d keys, got %d", len(tt.expected), len(keys))
			}

			for i, key := range keys {
				if key.Path != tt.expected[i] {
					t.Errorf("position %d: expected %q, got %q", i, tt.expected[i], key.Path)
				}
			}
		})
	}
}

// TestPriorityKeyEmptyBackups tests handling of files with no backup destinations.
// These should have the highest priority (destCount=0).
func TestPriorityKeyEmptyBackups(t *testing.T) {
	files := []fileData{
		{Size: 100, Fpath: core.Fpath("/backed_up.txt"), BackupDest: []string{"dest1"}},
		{Size: 5000, Fpath: core.Fpath("/never_backed_up.txt"), BackupDest: []string{}},
		{Size: 1000, Fpath: core.Fpath("/partially_backed.txt"), BackupDest: []string{"dest1", "dest2"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Never backed up should come first
	if keys[0].Path != "/never_backed_up.txt" {
		t.Errorf("first priority should be never-backed-up file, got %q", keys[0].Path)
	}
	if keys[0].DestCount != 0 {
		t.Errorf("never-backed-up file should have destCount=0, got %d", keys[0].DestCount)
	}
}

// TestPriorityKeyDuplicatePaths tests that files with identical paths but different backup
// destinations are handled correctly (the one with fewer destinations comes first).
func TestPriorityKeyDuplicatePaths(t *testing.T) {
	// Note: In real usage, the same path shouldn't have different sizes, but we test the ordering logic
	files := []fileData{
		{Size: 1000, Fpath: core.Fpath("/important.doc"), BackupDest: []string{"dest1", "dest2", "dest3"}},
		{Size: 1000, Fpath: core.Fpath("/important.doc"), BackupDest: []string{"dest1"}},
		{Size: 1000, Fpath: core.Fpath("/important.doc"), BackupDest: []string{"dest1", "dest2"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Should be ordered by destCount
	if keys[0].DestCount != 1 {
		t.Errorf("first should have destCount=1, got %d", keys[0].DestCount)
	}
	if keys[1].DestCount != 2 {
		t.Errorf("second should have destCount=2, got %d", keys[1].DestCount)
	}
	if keys[2].DestCount != 3 {
		t.Errorf("third should have destCount=3, got %d", keys[2].DestCount)
	}
}

// TestBuildPriorityKey tests the construction of priorityKey from fileData.
func TestBuildPriorityKey(t *testing.T) {
	tests := []struct {
		name          string
		fd            fileData
		expectedCount int
		expectedSize  uint64
		expectedPath  string
	}{
		{
			name:          "no backup destinations",
			fd:            fileData{Size: 1000, Fpath: core.Fpath("/file.txt"), BackupDest: []string{}},
			expectedCount: 0,
			expectedSize:  ^uint64(1000),
			expectedPath:  "/file.txt",
		},
		{
			name:          "multiple destinations",
			fd:            fileData{Size: 5000, Fpath: core.Fpath("/data.bin"), BackupDest: []string{"d1", "d2", "d3"}},
			expectedCount: 3,
			expectedSize:  ^uint64(5000),
			expectedPath:  "/data.bin",
		},
		{
			name:          "large file",
			fd:            fileData{Size: 1_000_000_000, Fpath: core.Fpath("/huge.iso"), BackupDest: []string{"external"}},
			expectedCount: 1,
			expectedSize:  ^uint64(1_000_000_000),
			expectedPath:  "/huge.iso",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := buildPriorityKey(tt.fd)

			if key.DestCount != tt.expectedCount {
				t.Errorf("DestCount: expected %d, got %d", tt.expectedCount, key.DestCount)
			}
			if key.InvSize != tt.expectedSize {
				t.Errorf("InvSize: expected %v, got %v", tt.expectedSize, key.InvSize)
			}
			if key.Path != tt.expectedPath {
				t.Errorf("Path: expected %q, got %q", tt.expectedPath, key.Path)
			}
		})
	}
}

// TestPriorityKeyEquals tests the Equals method.
func TestPriorityKeyEquals(t *testing.T) {
	key1 := priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"}
	key2 := priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"}
	key3 := priorityKey{DestCount: 2, InvSize: ^uint64(1000), Path: "/file.txt"}
	key4 := priorityKey{DestCount: 1, InvSize: ^uint64(2000), Path: "/file.txt"}
	key5 := priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/other.txt"}

	if !key1.Equals(key2) {
		t.Error("identical keys should be equal")
	}
	if key1.Equals(key3) {
		t.Error("keys with different DestCount should not be equal")
	}
	if key1.Equals(key4) {
		t.Error("keys with different InvSize should not be equal")
	}
	if key1.Equals(key5) {
		t.Error("keys with different Path should not be equal")
	}
}

// TestPriorityKeyMarshalUnmarshal tests JSON marshaling and unmarshaling.
func TestPriorityKeyMarshalUnmarshal(t *testing.T) {
	original := priorityKey{DestCount: 3, InvSize: ^uint64(5000), Path: "/data/file.txt"}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var restored priorityKey
	if err := restored.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if !original.Equals(restored) {
		t.Errorf("restored key doesn't match original: %+v != %+v", original, restored)
	}
}

// TestPriorityKeyLessComparator tests the less-than comparator directly.
func TestPriorityKeyLessComparator(t *testing.T) {
	tests := []struct {
		name     string
		a        priorityKey
		b        priorityKey
		expected bool // true if a < b
	}{
		{
			name:     "fewer destinations comes first",
			a:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"},
			b:        priorityKey{DestCount: 2, InvSize: ^uint64(1000), Path: "/file.txt"},
			expected: true,
		},
		{
			name:     "more destinations comes second",
			a:        priorityKey{DestCount: 2, InvSize: ^uint64(1000), Path: "/file.txt"},
			b:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"},
			expected: false,
		},
		{
			name:     "larger file comes first (smaller invSize)",
			a:        priorityKey{DestCount: 1, InvSize: ^uint64(5000), Path: "/file.txt"},
			b:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"},
			expected: true,
		},
		{
			name:     "smaller file comes second (larger invSize)",
			a:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"},
			b:        priorityKey{DestCount: 1, InvSize: ^uint64(5000), Path: "/file.txt"},
			expected: false,
		},
		{
			name:     "alphabetically earlier path comes first",
			a:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/apple.txt"},
			b:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/zebra.txt"},
			expected: true,
		},
		{
			name:     "alphabetically later path comes second",
			a:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/zebra.txt"},
			b:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/apple.txt"},
			expected: false,
		},
		{
			name:     "identical keys not less than",
			a:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"},
			b:        priorityKey{DestCount: 1, InvSize: ^uint64(1000), Path: "/file.txt"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := priorityKeyLess(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("priorityKeyLess(%+v, %+v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestPriorityKeyBackupPriority is a realistic scenario test demonstrating backup priority.
// This test shows how files get prioritized in a real backup scenario.
func TestPriorityKeyBackupPriority(t *testing.T) {
	// Simulating a backup scenario with various files in different states
	files := []fileData{
		// User's important documents - never backed up
		{Size: 50_000, Fpath: core.Fpath("/home/user/documents/resume.pdf"), BackupDest: []string{}},
		{Size: 30_000, Fpath: core.Fpath("/home/user/documents/tax_return.pdf"), BackupDest: []string{}},

		// Media files - partially backed up
		{Size: 5_000_000, Fpath: core.Fpath("/media/video.mp4"), BackupDest: []string{"usb_drive"}},
		{Size: 3_000_000, Fpath: core.Fpath("/media/photos.zip"), BackupDest: []string{"cloud"}},

		// Source code - already backed up everywhere
		{Size: 100_000, Fpath: core.Fpath("/code/project.tar.gz"), BackupDest: []string{"github", "gitlab", "local_backup"}},

		// Log files - backed up in one place
		{Size: 10_000, Fpath: core.Fpath("/var/log/app.log"), BackupDest: []string{"backup_server"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Expected priority order:
	// 1. Files with 0 backups (largest first)
	// 2. Files with 1 backup (largest first)
	// 3. Files with 3 backups
	expected := []string{
		"/home/user/documents/resume.pdf",     // 0 backups, 50KB
		"/home/user/documents/tax_return.pdf", // 0 backups, 30KB
		"/media/video.mp4",                    // 1 backup, 5MB
		"/media/photos.zip",                   // 1 backup, 3MB
		"/var/log/app.log",                    // 1 backup, 10KB
		"/code/project.tar.gz",                // 3 backups
	}

	for i, key := range keys {
		if key.Path != expected[i] {
			t.Errorf("position %d: expected %q, got %q", i, expected[i], key.Path)
		}
	}
}

// TestPriorityKeyWindowsPaths tests handling of Windows-style file paths with backslashes.
// This ensures the priority key works correctly with Windows path conventions.
func TestPriorityKeyWindowsPaths(t *testing.T) {
	files := []fileData{
		{Size: 100, Fpath: core.Fpath(`C:\Users\John\Documents\file.txt`), BackupDest: []string{}},
		{Size: 5000, Fpath: core.Fpath(`D:\Media\Video.mp4`), BackupDest: []string{"backup1"}},
		{Size: 1000, Fpath: core.Fpath(`C:\Users\Jane\file.txt`), BackupDest: []string{}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Both 0-backup files should come first, ordered by size (larger first)
	if keys[0].DestCount != 0 {
		t.Errorf("first should have 0 backups, got %d", keys[0].DestCount)
	}
	if keys[1].DestCount != 0 {
		t.Errorf("second should have 0 backups, got %d", keys[1].DestCount)
	}
	// Larger file (5000) should be prioritized over smaller (1000) even though destCount differs
	// 0 backups (5000 bytes) > 0 backups (1000 bytes)
	if keys[0].InvSize > keys[1].InvSize {
		t.Error("larger file should come before smaller file with same destCount")
	}
}

// TestPriorityKeyUNCPaths tests UNC network paths (\\server\share format).
func TestPriorityKeyUNCPaths(t *testing.T) {
	files := []fileData{
		{Size: 1000, Fpath: core.Fpath(`\\server1\share\file.txt`), BackupDest: []string{"local"}},
		{Size: 2000, Fpath: core.Fpath(`\\server2\backup\data.zip`), BackupDest: []string{}},
		{Size: 500, Fpath: core.Fpath(`\\server1\share\readme.md`), BackupDest: []string{"cloud"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// File with no backups should come first
	if keys[0].DestCount != 0 {
		t.Errorf("first should have no backups, got %d", keys[0].DestCount)
	}
	if keys[0].Path != `\\server2\backup\data.zip` {
		t.Errorf("file with no backups should be prioritized, got %q", keys[0].Path)
	}
}

// TestPriorityKeyWindowsSpecialCharacters tests Windows filenames with special characters.
// Windows allows many special characters in filenames except: < > : " / \ | ? *
func TestPriorityKeyWindowsSpecialCharacters(t *testing.T) {
	files := []fileData{
		// File with spaces (very common on Windows)
		{Size: 1000, Fpath: core.Fpath(`C:\My Documents\My File.txt`), BackupDest: []string{}},
		// File with multiple dots
		{Size: 2000, Fpath: core.Fpath(`C:\backup.2024.01.05.tar.gz`), BackupDest: []string{}},
		// File with parentheses and brackets
		{Size: 500, Fpath: core.Fpath(`C:\data\[important](final).xlsx`), BackupDest: []string{"dest1"}},
		// File with @ and # symbols
		{Size: 1500, Fpath: core.Fpath(`C:\mail\user@domain#2024.eml`), BackupDest: []string{}},
		// File with ampersand and dash
		{Size: 800, Fpath: core.Fpath(`C:\code\my-library_v2.0 & utils.dll`), BackupDest: []string{"backup"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Three files have 0 backups, two have 1 backup
	// Expect 0-backup files to come first (indices 0-2), ordered by size: 2000, 1500, 1000
	zeroBackupCount := 0
	for i := 0; i < len(keys); i++ {
		if keys[i].DestCount == 0 {
			zeroBackupCount++
		} else {
			break // should be contiguous
		}
	}
	if zeroBackupCount != 3 {
		t.Errorf("expected 3 files with 0 backups at start, got %d", zeroBackupCount)
	}

	// Verify the 0-backup files are ordered by size (descending: 2000 > 1500 > 1000)
	if keys[0].InvSize >= keys[1].InvSize || keys[1].InvSize >= keys[2].InvSize {
		t.Error("zero-backup files should be ordered by size descending")
	}

	// Files with 1 backup should come after
	if keys[3].DestCount != 1 || keys[4].DestCount != 1 {
		t.Error("remaining files should have 1 backup")
	}
}

// TestPriorityKeyWindowsReservedNames tests handling of Windows reserved/problematic filenames.
// While Windows typically prevents creating files with these names, the path ordering should still work.
func TestPriorityKeyWindowsReservedNames(t *testing.T) {
	// Note: In practice, Windows prevents creating files with these exact names
	// (CON, PRN, AUX, NUL, COM1-COM9, LPT1-LPT9), but we test that the ordering logic
	// handles them consistently if they somehow exist in the system being backed up
	files := []fileData{
		{Size: 1000, Fpath: core.Fpath(`C:\System\CON.bak`), BackupDest: []string{}},
		{Size: 2000, Fpath: core.Fpath(`C:\System\PRN.tmp`), BackupDest: []string{}},
		{Size: 500, Fpath: core.Fpath(`C:\System\AUX.log`), BackupDest: []string{"archive"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Files with no backups come first (first 2 files)
	if keys[0].DestCount != 0 || keys[1].DestCount != 0 {
		t.Error("files with no backups should come first")
	}
	// Should be ordered by size (2000 > 1000)
	if keys[0].InvSize > keys[1].InvSize {
		t.Error("larger reserved file should come first")
	}
	// File with backups comes last
	if keys[2].DestCount != 1 {
		t.Errorf("file with backup should be last, got %d backups", keys[2].DestCount)
	}
}

// TestPriorityKeyMixedPathFormats tests mixing Unix and Windows path styles.
// While unusual, medorg should handle this gracefully.
func TestPriorityKeyMixedPathFormats(t *testing.T) {
	files := []fileData{
		{Size: 1000, Fpath: core.Fpath(`C:\Users\file.txt`), BackupDest: []string{}},
		{Size: 2000, Fpath: core.Fpath(`/home/user/file.txt`), BackupDest: []string{}},
		{Size: 1500, Fpath: core.Fpath(`C:/Users/alternative/file.txt`), BackupDest: []string{"dest1"}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Zero-backup files should come first
	if keys[0].DestCount != 0 || keys[1].DestCount != 0 {
		t.Error("files with no backups should come first")
	}
	// Ordered by size: 2000 > 1000
	if keys[0].InvSize > keys[1].InvSize {
		t.Error("larger file should come first")
	}
}

// TestPriorityKeyWindowsLongPaths tests very long Windows file paths.
// Windows 10+ supports paths up to 260 characters by default, up to 32,767 with special handling.
func TestPriorityKeyWindowsLongPaths(t *testing.T) {
	// Construct realistic long paths
	longPath1 := `C:\Users\Documents\ProjectA\SubDir\VeryDeepFolder\AnotherLevel\AndAnotherOne\WithManySubdirs\FinalFolder\file_with_long_name.txt`
	longPath2 := `C:\Archive\Backup\2024\January\Week1\Day1\Hour1\Minute1\backup_archive_2024_01_05_120000_full.tar.gz`
	longPath3 := `D:\MediaLibrary\Movies\2024\Hollywood\Dramas\ActionFilms\Director_Unknown\movie_file_version_1_with_commentary_and_subtitles_english_french.mkv`

	files := []fileData{
		{Size: 50000, Fpath: core.Fpath(longPath1), BackupDest: []string{}},
		{Size: 100000, Fpath: core.Fpath(longPath2), BackupDest: []string{"external_drive"}},
		{Size: 75000, Fpath: core.Fpath(longPath3), BackupDest: []string{}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// Files with no backups should come first
	if keys[0].DestCount != 0 || keys[1].DestCount != 0 {
		t.Error("files with no backups should come first")
	}
	// Among 0-backup files, largest should be first (100000 > 50000)
	// Wait, keys[0] and keys[1] are the 0-backup files, but longPath2 has 1 backup
	// Let me reconsider: longPath1 (50KB, 0 backups) and longPath3 (75KB, 0 backups) are 0-backup
	// longPath2 (100KB, 1 backup) has 1 backup
	if keys[0].DestCount != 0 {
		t.Errorf("first should have 0 backups, got %d", keys[0].DestCount)
	}
	if keys[1].DestCount != 0 {
		t.Errorf("second should have 0 backups, got %d", keys[1].DestCount)
	}
	// Among 0-backup files, 75000 > 50000, so longPath3 should come before longPath1
	if keys[0].InvSize > keys[1].InvSize {
		t.Error("larger file should come first among same backup count")
	}
	// File with 1 backup should be last
	if keys[2].DestCount != 1 {
		t.Errorf("last file should have 1 backup, got %d", keys[2].DestCount)
	}
}

// TestPriorityKeyWindowsCaseInsensitivity tests that paths are ordered lexicographically
// (case-sensitive ordering, though Windows filesystem is case-insensitive).
func TestPriorityKeyWindowsCaseInsensitivity(t *testing.T) {
	files := []fileData{
		{Size: 1000, Fpath: core.Fpath(`C:\Users\Zebra.txt`), BackupDest: []string{}},
		{Size: 1000, Fpath: core.Fpath(`C:\Users\apple.txt`), BackupDest: []string{}},
		{Size: 1000, Fpath: core.Fpath(`C:\Users\Banana.txt`), BackupDest: []string{}},
	}

	keys := make([]priorityKey, len(files))
	for i, fd := range files {
		keys[i] = buildPriorityKey(fd)
	}

	sort.Slice(keys, func(i, j int) bool {
		return priorityKeyLess(keys[i], keys[j])
	})

	// All have same destCount and size, so should be ordered by path lexicographically
	// Lexicographic order: "Banana" < "Zebra" < "apple" (uppercase comes before lowercase in ASCII)
	expectedOrder := []string{`C:\Users\Banana.txt`, `C:\Users\Zebra.txt`, `C:\Users\apple.txt`}
	for i, expected := range expectedOrder {
		if keys[i].Path != expected {
			t.Errorf("position %d: expected %q, got %q", i, expected, keys[i].Path)
		}
	}
}
