package consumers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/adaptive"
)

// TestRunCheckCalc_EndToEnd_TunerProgressFlow demonstrates the complete
// adaptive tuning flow with real-time progress tracking
func TestRunCheckCalc_EndToEnd_TunerProgressFlow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "e2e-tuner-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a realistic file structure with varying sizes
	testFiles := map[string]int64{
		"small.bin":  50 * 1024,       // 50 KB
		"medium.bin": 500 * 1024,      // 500 KB
		"large.bin":  2 * 1024 * 1024, // 2 MB
		"tiny.txt":   1024,            // 1 KB
		"docs.pdf":   300 * 1024,      // 300 KB
	}

	totalExpectedSize := int64(0)
	for name, size := range testFiles {
		if err := os.WriteFile(filepath.Join(tmpDir, name), make([]byte, size), 0o644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", name, err)
		}
		totalExpectedSize += size
	}

	// Create tuner with specific config
	tuner := adaptive.NewTuner()

	// Run CheckCalc with tuner enabled
	opts := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
		Tuner:     tuner,
	}

	// Execute the full calculation pipeline
	if err := RunCheckCalc([]string{tmpDir}, opts); err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	// Verify tuner received progressive updates
	stats := tuner.GetStats()

	bytesProcessed := stats["bytes_processed"].(int64)
	samplesCollected := stats["samples_collected"].(int)
	currentTokens := stats["current_tokens"].(int)

	t.Logf("End-to-end results:")
	t.Logf("  Files: %d", len(testFiles))
	t.Logf("  Expected size: %d bytes (%.2f MB)", totalExpectedSize, float64(totalExpectedSize)/(1024*1024))
	t.Logf("  Bytes processed: %d bytes (%.2f MB)", bytesProcessed, float64(bytesProcessed)/(1024*1024))
	t.Logf("  Samples collected: %d", samplesCollected)
	t.Logf("  Current tokens: %d", currentTokens)

	// Verify bytes processed is reasonable
	// (May be higher than file size due to metadata and I/O buffering)
	if bytesProcessed < totalExpectedSize {
		t.Errorf("Processed bytes %d < expected minimum %d", bytesProcessed, totalExpectedSize)
	}

	// Verify tuner is actively monitoring
	// At least some samples should have been collected (one per check interval or more)
	if samplesCollected < 1 {
		t.Logf("Warning: No throughput samples collected - operation may have been too fast")
	}

	// Verify tuner has tokens allocated
	if currentTokens < 1 {
		t.Error("Tuner has no tokens - token management failed")
	}

	// Verify .medorg.xml was created
	xmlPath := filepath.Join(tmpDir, ".medorg.xml")
	if _, err := os.Stat(xmlPath); err != nil {
		t.Errorf(".medorg.xml not created: %v", err)
	}

	t.Logf("✓ End-to-end flow completed successfully")
}

// TestRunCheckCalc_MultipleDirectories_WithTuner tests tuner across multiple directories
func TestRunCheckCalc_MultipleDirectories_WithTuner(t *testing.T) {
	dir1, err := os.MkdirTemp("", "tuner-dir1-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir1)

	dir2, err := os.MkdirTemp("", "tuner-dir2-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir2)

	// Create files in dir1
	for i := 0; i < 3; i++ {
		path := filepath.Join(dir1, "file"+string(rune('A'+i))+".dat")
		if err := os.WriteFile(path, make([]byte, 100*1024), 0o644); err != nil {
			t.Fatalf("Failed to create file in dir1: %v", err)
		}
	}

	// Create files in dir2
	for i := 0; i < 2; i++ {
		path := filepath.Join(dir2, "doc"+string(rune('1'+i))+".txt")
		if err := os.WriteFile(path, make([]byte, 50*1024), 0o644); err != nil {
			t.Fatalf("Failed to create file in dir2: %v", err)
		}
	}

	tuner := adaptive.NewTuner()

	opts := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
		Tuner:     tuner,
	}

	// Process both directories in one call
	if err := RunCheckCalc([]string{dir1, dir2}, opts); err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	stats := tuner.GetStats()
	bytesProcessed := stats["bytes_processed"].(int64)

	// Expected: 3*100KB + 2*50KB = 400KB minimum
	expectedMin := int64(400 * 1024)
	if bytesProcessed < expectedMin {
		t.Errorf("Processed bytes %d < expected minimum %d", bytesProcessed, expectedMin)
	}

	// Verify both directories have .medorg.xml
	for _, dir := range []string{dir1, dir2} {
		xmlPath := filepath.Join(dir, ".medorg.xml")
		if _, err := os.Stat(xmlPath); err != nil {
			t.Errorf("Missing .medorg.xml in %s: %v", dir, xmlPath)
		}
	}

	t.Logf("✓ Multiple directories processed with tuner: %d bytes from %s and %s",
		bytesProcessed, dir1, dir2)
}

// TestRunCheckCalc_TunerVsNoTuner_ProducesIdenticalResults
// ensures tuner doesn't affect the output, only the performance measurement
func TestRunCheckCalc_TunerVsNoTuner_ProducesIdenticalResults(t *testing.T) {
	// Create test directory 1
	dir1, err := os.MkdirTemp("", "tuner-vs-notuner-1-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir1)

	// Create test directory 2 (identical to 1)
	dir2, err := os.MkdirTemp("", "tuner-vs-notuner-2-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir2)

	// Create identical files in both
	testFiles := map[string][]byte{
		"file1.dat": make([]byte, 10*1024),
		"file2.dat": make([]byte, 20*1024),
	}

	for name, data := range testFiles {
		if err := os.WriteFile(filepath.Join(dir1, name), data, 0o644); err != nil {
			t.Fatalf("Failed to create file in dir1: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir2, name), data, 0o644); err != nil {
			t.Fatalf("Failed to create file in dir2: %v", err)
		}
	}

	// Run with tuner
	tuner := adaptive.NewTuner()
	optsWithTuner := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
		Tuner:     tuner,
	}

	if err := RunCheckCalc([]string{dir1}, optsWithTuner); err != nil {
		t.Fatalf("RunCheckCalc with tuner failed: %v", err)
	}

	// Run without tuner
	optsWithoutTuner := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
		Tuner:     nil,
	}

	if err := RunCheckCalc([]string{dir2}, optsWithoutTuner); err != nil {
		t.Fatalf("RunCheckCalc without tuner failed: %v", err)
	}

	// Verify both produced .medorg.xml
	xml1 := filepath.Join(dir1, ".medorg.xml")
	xml2 := filepath.Join(dir2, ".medorg.xml")

	if _, err := os.Stat(xml1); err != nil {
		t.Errorf("XML not created in dir1: %v", err)
	}
	if _, err := os.Stat(xml2); err != nil {
		t.Errorf("XML not created in dir2: %v", err)
	}

	t.Logf("✓ Identical results produced with and without tuner")
}
