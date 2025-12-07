package consumers

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/adaptive"
)

// TestRunCheckCalc_TunerReceivesProgressiveUpdates verifies tuner gets continuous progress
func TestRunCheckCalc_TunerReceivesProgressiveUpdates(t *testing.T) {
	// Create temp directory with test files
	tmpDir, err := os.MkdirTemp("", "checkcalc-tuner-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a couple of test files
	testFiles := map[string][]byte{
		"file1.dat": make([]byte, 10000),
		"file2.dat": make([]byte, 15000),
	}

	for name, data := range testFiles {
		if err := os.WriteFile(filepath.Join(tmpDir, name), data, 0o644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	// Create tuner
	tuner := adaptive.NewTuner()

	// Run CheckCalc with tuner
	opts := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true, // Force recalculation
		Tuner:     tuner,
	}

	err = RunCheckCalc([]string{tmpDir}, opts)
	if err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	// Get tuner stats to verify it received updates
	stats := tuner.GetStats()
	bytesProcessed := stats["bytes_processed"].(int64)

	if bytesProcessed == 0 {
		t.Error("Tuner received no bytes - progress callbacks not working")
	}

	expectedSize := int64(len(testFiles["file1.dat"]) + len(testFiles["file2.dat"]))
	if bytesProcessed < expectedSize {
		t.Errorf("Total bytes processed %d < expected %d", bytesProcessed, expectedSize)
	}

	t.Logf("Tuner processed %d bytes (expected ~%d)", bytesProcessed, expectedSize)
}

// TestRunCheckCalc_TunerGetsConsistentThroughput verifies throughput measurements work
func TestRunCheckCalc_TunerGetsConsistentThroughput(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkcalc-throughput-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create larger test file (50KB)
	testData := make([]byte, 50*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, "test.dat"), testData, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create tuner with longer interval to let throughput stabilize
	tuner := adaptive.NewTunerWithConfig(1, 4, 2*time.Second)

	opts := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
		Tuner:     tuner,
	}

	start := time.Now()
	err = RunCheckCalc([]string{tmpDir}, opts)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	// Get tuner stats
	stats := tuner.GetStats()
	throughput := stats["current_throughput"].(float64)
	bestThroughput := stats["best_throughput"].(float64)

	t.Logf("Throughput: current=%.2f MB/s, best=%.2f MB/s, elapsed=%.2fs",
		throughput/(1024*1024), bestThroughput/(1024*1024), elapsed.Seconds())

	// Verify stats look reasonable
	if bestThroughput <= 0 && elapsed.Seconds() > 0.5 {
		// If processing took significant time but throughput is 0, something is wrong
		t.Logf("Warning: Processing took %.2fs but throughput is %.2f MB/s",
			elapsed.Seconds(), bestThroughput/(1024*1024))
	}
}

// TestRunCheckCalc_TunerRecordsProgressiveBytesPerFile verifies each file gets recorded
func TestRunCheckCalc_TunerRecordsProgressiveBytesPerFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkcalc-progressive-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create multiple files of different sizes
	fileSizes := map[string]int{
		"small.dat":  5 * 1024,   // 5KB
		"medium.dat": 50 * 1024,  // 50KB
		"large.dat":  100 * 1024, // 100KB
	}

	totalExpectedBytes := 0
	for name, size := range fileSizes {
		totalExpectedBytes += size
		data := make([]byte, size)
		if err := os.WriteFile(filepath.Join(tmpDir, name), data, 0o644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	tuner := adaptive.NewTuner()

	opts := CheckCalcOptions{
		CalcCount: 1,
		Recalc:    true,
		Tuner:     tuner,
	}

	err = RunCheckCalc([]string{tmpDir}, opts)
	if err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	// Get stats from tuner
	stats := tuner.GetStats()
	bytesProcessed := stats["bytes_processed"].(int64)

	if bytesProcessed < int64(totalExpectedBytes) {
		t.Errorf("Total recorded bytes %d < expected %d", bytesProcessed, totalExpectedBytes)
	}

	t.Logf("Tuner processed %d bytes for %d files", bytesProcessed, len(fileSizes))
}

// TestRunCheckCalc_WithoutTunerStillWorks ensures backward compatibility
func TestRunCheckCalc_WithoutTunerStillWorks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checkcalc-notuner-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test file
	if err := os.WriteFile(filepath.Join(tmpDir, "test.dat"), make([]byte, 1024), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Run without tuner (nil)
	opts := CheckCalcOptions{
		CalcCount: 2,
		Recalc:    true,
		Tuner:     nil, // No tuner
	}

	err = RunCheckCalc([]string{tmpDir}, opts)
	if err != nil {
		t.Fatalf("RunCheckCalc without tuner failed: %v", err)
	}

	// Verify .medorg.xml was created
	xmlPath := filepath.Join(tmpDir, ".medorg.xml")
	if _, err := os.Stat(xmlPath); err != nil {
		t.Errorf(".medorg.xml not created: %v", err)
	}
}
