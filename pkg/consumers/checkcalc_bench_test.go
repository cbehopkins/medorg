package consumers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// setupBenchmarkTree creates a directory tree with the specified structure
// and optionally pre-calculates checksums. Returns the root directory path.
//
// Structure: 3 levels deep, 3 subdirectories per level, 5 files per directory
// Total: ~40 directories, ~200 files
func setupBenchmarkTree(b *testing.B, precalculate bool) string {
	tmpRoot := b.TempDir()

	// Helper to create files in a directory
	createFiles := func(dir string, count int) {
		for i := range count {
			filename := filepath.Join(dir, filepath.Base(dir)+"-file"+string(rune('a'+i))+".dat")
			// Create files of varying sizes (1KB to 100KB)
			size := 1024 + (i * 20 * 1024)
			data := make([]byte, size)
			// Add some varying content to avoid identical checksums
			for j := 0; j < len(data); j++ {
				data[j] = byte(j % 256)
			}
			if err := os.WriteFile(filename, data, 0o644); err != nil {
				b.Fatalf("Failed to create file %s: %v", filename, err)
			}
		}
	}

	// Build tree structure: 3 levels deep
	var dirs []string
	dirs = append(dirs, tmpRoot)

	// Level 1: 3 subdirectories
	for i := range 3 {
		level1 := filepath.Join(tmpRoot, "dir1-"+string(rune('a'+i)))
		if err := os.MkdirAll(level1, 0o755); err != nil {
			b.Fatalf("Failed to create directory: %v", err)
		}
		dirs = append(dirs, level1)

		// Level 2: 3 subdirectories per level 1 dir
		for j := range 3 {
			level2 := filepath.Join(level1, "dir2-"+string(rune('a'+j)))
			if err := os.MkdirAll(level2, 0o755); err != nil {
				b.Fatalf("Failed to create directory: %v", err)
			}
			dirs = append(dirs, level2)

			// Level 3: 3 subdirectories per level 2 dir
			for k := range 3 {
				level3 := filepath.Join(level2, "dir3-"+string(rune('a'+k)))
				if err := os.MkdirAll(level3, 0o755); err != nil {
					b.Fatalf("Failed to create directory: %v", err)
				}
				dirs = append(dirs, level3)
			}
		}
	}

	// Create files in each directory
	for _, dir := range dirs {
		createFiles(dir, 5)
	}

	// If requested, pre-calculate all checksums
	if precalculate {
		opts := CheckCalcOptions{
			CalcCount: 2,
			Recalc:    true, // Force calculation of all files
		}
		if err := RunCheckCalc([]string{tmpRoot}, opts); err != nil {
			b.Fatalf("Failed to pre-calculate checksums: %v", err)
		}
	}

	return tmpRoot
}

// touchFilesForUpdate modifies a few files scattered across the tree
// to simulate files that need checksum updates
func touchFilesForUpdate(b *testing.B, root string) {
	// Find some files to touch - we'll pick specific patterns
	patterns := []string{
		filepath.Join(root, "dir1-a", "dir2-a", "dir3-a", "dir3-a-filea.dat"),
		filepath.Join(root, "dir1-b", "dir2-b", "dir3-b", "dir3-b-fileb.dat"),
		filepath.Join(root, "dir1-c", "dir2-a", "dir3-c", "dir3-c-filec.dat"),
		filepath.Join(root, "root-fileb.dat"),
		filepath.Join(root, "dir1-a", "dir1-a-filec.dat"),
	}

	for _, path := range patterns {
		// Read existing file
		data, err := os.ReadFile(path)
		if err != nil {
			// File might not exist in every run, skip gracefully
			continue
		}

		// Modify content slightly
		if len(data) > 10 {
			data[10] = byte((int(data[10]) + 1) % 256)
		}

		// Write it back
		if err := os.WriteFile(path, data, 0o644); err != nil {
			b.Fatalf("Failed to touch file %s: %v", path, err)
		}
	}
}

// BenchmarkRunCheckCalc_AllChecksumsCached measures walk & XML read performance
// when all checksums are already up-to-date. This benchmarks the overhead of:
// - Directory tree walking
// - Loading .medorg.xml files
// - Comparing file mtimes against cached checksums
// - No actual MD5 calculations should occur
func BenchmarkRunCheckCalc_AllChecksumsCached(b *testing.B) {
	// Setup: Create tree with pre-calculated checksums
	root := setupBenchmarkTree(b, true)

	// Verify setup worked - load one of the XML files to confirm
	dm, err := core.DirectoryMapFromDir(core.Dirname(root), nil)
	if err != nil {
		b.Fatalf("Failed to load DirectoryMap from root: %v", err)
	}
	if dm.Len() == 0 {
		b.Fatal("No files found in pre-calculated tree - setup failed")
	}

	opts := CheckCalcOptions{
		CalcCount:    2,
		Recalc:       false, // Don't force recalc - use cached values
		ShowProgress: false,
	}

	// Reset timer after setup
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		if err := RunCheckCalc([]string{root}, opts); err != nil {
			b.Fatalf("RunCheckCalc failed: %v", err)
		}
	}

	b.StopTimer()

	// Report tree statistics
	b.ReportMetric(float64(dm.Len()), "files_in_root")
}

// BenchmarkRunCheckCalc_SparseMTIMEUpdates measures performance when a few
// files scattered across the tree have been modified and need rechecksumming.
// This benchmarks:
// - Directory tree walking
// - Loading .medorg.xml files
// - Detecting mtime changes on a subset of files
// - Calculating MD5 for only the modified files
// - Updating XML files with new checksums
func BenchmarkRunCheckCalc_SparseMTIMEUpdates(b *testing.B) {
	// Setup: Create tree with pre-calculated checksums
	root := setupBenchmarkTree(b, true)

	opts := CheckCalcOptions{
		CalcCount:    2,
		Recalc:       false,
		ShowProgress: false,
	}

	// Reset timer after initial setup
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Stop timer while we modify files
		b.StopTimer()

		// Touch a handful of files to trigger recalculation
		touchFilesForUpdate(b, root)

		// Resume timing for the actual CheckCalc operation
		b.StartTimer()

		if err := RunCheckCalc([]string{root}, opts); err != nil {
			b.Fatalf("RunCheckCalc failed: %v", err)
		}
	}
}

// BenchmarkRunCheckCalc_EmptyTree measures baseline performance on an empty tree
func BenchmarkRunCheckCalc_EmptyTree(b *testing.B) {
	tmpRoot := b.TempDir()

	opts := CheckCalcOptions{
		CalcCount: 2,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := RunCheckCalc([]string{tmpRoot}, opts); err != nil {
			b.Fatalf("RunCheckCalc failed: %v", err)
		}
	}
}

// BenchmarkRunCheckCalc_FlatDirectory measures performance on a single flat
// directory with many files (no subdirectories)
func BenchmarkRunCheckCalc_FlatDirectory(b *testing.B) {
	tmpRoot := b.TempDir()

	// Create 100 files in a single directory
	for i := 0; i < 100; i++ {
		filename := filepath.Join(tmpRoot, "file"+string(rune('0'+(i/10)))+string(rune('0'+(i%10)))+".dat")
		// 10KB files
		data := make([]byte, 10*1024)
		for j := 0; j < len(data); j++ {
			data[j] = byte(j % 256)
		}
		if err := os.WriteFile(filename, data, 0o644); err != nil {
			b.Fatalf("Failed to create file: %v", err)
		}
	}

	// Pre-calculate checksums
	opts := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
	}
	if err := RunCheckCalc([]string{tmpRoot}, opts); err != nil {
		b.Fatalf("Failed to pre-calculate: %v", err)
	}

	// Now benchmark with cached checksums
	opts.Recalc = false

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := RunCheckCalc([]string{tmpRoot}, opts); err != nil {
			b.Fatalf("RunCheckCalc failed: %v", err)
		}
	}
}
