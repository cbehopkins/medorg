package core

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCalcMd5FileWithProgress_ProgressCallbackCalled verifies callback is invoked during processing
func TestCalcMd5FileWithProgress_ProgressCallbackCalled(t *testing.T) {
	// Create a test file
	tmpDir, err := os.MkdirTemp("", "checksum-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test file with known content
	testFile := filepath.Join(tmpDir, "test.bin")
	testData := bytes.Repeat([]byte("Hello, World!"), 1000) // ~13KB
	if err := os.WriteFile(testFile, testData, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	callCount := atomic.Int32{}
	var totalBytes int64
	var callbacks []struct {
		bytes int64
		time  time.Time
	}
	var mu sync.Mutex

	callback := func(bytesProcessed int64, timestamp time.Time) {
		callCount.Add(1)
		mu.Lock()
		defer mu.Unlock()
		callbacks = append(callbacks, struct {
			bytes int64
			time  time.Time
		}{bytesProcessed, timestamp})
		totalBytes += bytesProcessed // Accumulate delta bytes to get total
	}

	checksum, err := CalcMd5FileWithProgress(tmpDir, "test.bin", callback)
	if err != nil {
		t.Fatalf("CalcMd5FileWithProgress failed: %v", err)
	}

	// Verify callback was called at least once
	if callCount.Load() == 0 {
		t.Error("Progress callback was never called")
	}

	// Verify total bytes matches file size (accumulating deltas)
	if totalBytes != int64(len(testData)) {
		t.Errorf("Expected total bytes %d, got %d", len(testData), totalBytes)
	}

	// Verify checksum is non-empty
	if checksum == "" {
		t.Error("Checksum is empty")
	}

	// Verify callbacks show progression (delta bytes should all be positive)
	mu.Lock()
	defer mu.Unlock()
	for i, cb := range callbacks {
		if cb.bytes <= 0 {
			t.Errorf("Callback %d reported non-positive bytes: %d", i, cb.bytes)
		}
	}
}

// TestCalcMd5FileWithProgress_BytesAccumulate verifies bytes progress reports deltas correctly
func TestCalcMd5FileWithProgress_BytesAccumulate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checksum-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a larger test file to ensure multiple callbacks
	testFile := filepath.Join(tmpDir, "large.bin")
	testData := bytes.Repeat([]byte("X"), 1000*1000) // 1MB
	if err := os.WriteFile(testFile, testData, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	var callbacks []int64
	var totalBytes int64
	var mu sync.Mutex

	callback := func(bytesProcessed int64, timestamp time.Time) {
		mu.Lock()
		defer mu.Unlock()
		callbacks = append(callbacks, bytesProcessed)
		totalBytes += bytesProcessed // Accumulate deltas
	}

	_, err = CalcMd5FileWithProgress(tmpDir, "large.bin", callback)
	if err != nil {
		t.Fatalf("CalcMd5FileWithProgress failed: %v", err)
	}

	// Should have at least 2 callbacks (start and end)
	if len(callbacks) < 2 {
		t.Logf("Only %d callback(s) received (1MB file)", len(callbacks))
	}

	// Verify all deltas are positive
	for i, bytes := range callbacks {
		if bytes <= 0 {
			t.Errorf("Callback %d reported non-positive bytes: %d", i, bytes)
		}
	}

	// Verify total accumulated bytes matches file size
	if totalBytes != int64(len(testData)) {
		t.Errorf("Total accumulated bytes %d != file size %d", totalBytes, len(testData))
	}
}

// TestCalcMd5FileWithProgress_ProducesCorrectChecksum verifies calculation is correct
func TestCalcMd5FileWithProgress_ProducesCorrectChecksum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checksum-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.bin")
	testData := []byte("The quick brown fox jumps over the lazy dog")
	if err := os.WriteFile(testFile, testData, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Calculate with progress
	checksumWithProgress, err := CalcMd5FileWithProgress(tmpDir, "test.bin", func(int64, time.Time) {})
	if err != nil {
		t.Fatalf("CalcMd5FileWithProgress failed: %v", err)
	}

	// Calculate without progress
	checksumWithout, err := CalcMd5File(tmpDir, "test.bin")
	if err != nil {
		t.Fatalf("CalcMd5File failed: %v", err)
	}

	// Both should match
	if checksumWithProgress != checksumWithout {
		t.Errorf("Checksums don't match: with progress=%s, without=%s",
			checksumWithProgress, checksumWithout)
	}
}

// TestCalcMd5FileWithProgress_NilCallback handles nil callback gracefully
func TestCalcMd5FileWithProgress_NilCallback(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "checksum-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Should not panic with nil callback
	checksum, err := CalcMd5FileWithProgress(tmpDir, "test.bin", nil)
	if err != nil {
		t.Fatalf("CalcMd5FileWithProgress with nil callback failed: %v", err)
	}

	if checksum == "" {
		t.Error("Checksum should not be empty")
	}
}

// TestProgressReader_ContinuousUpdates verifies progressReader provides continuous updates (delta bytes)
func TestProgressReader_ContinuousUpdates(t *testing.T) {
	// Create a buffer with known data
	testData := bytes.Repeat([]byte("X"), 10000)
	reader := bytes.NewReader(testData)

	var bytesReports []int64
	var totalBytes int64
	var mu sync.Mutex

	pr := &progressReader{
		reader: reader,
		hash:   nil, // Not needed for this test, we just care about read tracking
		callback: func(bytes int64, _ time.Time) {
			mu.Lock()
			defer mu.Unlock()
			bytesReports = append(bytesReports, bytes)
			totalBytes += bytes // Accumulate deltas
		},
	}

	// Manually read to simulate file processing
	buffer := make([]byte, 1024)
	totalRead := 0
	for {
		n, err := pr.Read(buffer)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("progressReader.Read failed: %v", err)
		}
	}

	// Verify we got bytes reports
	if len(bytesReports) == 0 {
		t.Error("No byte reports received")
	}

	// Verify all reports are positive (deltas)
	mu.Lock()
	for i, bytes := range bytesReports {
		if bytes <= 0 {
			t.Errorf("Report %d had non-positive bytes: %d", i, bytes)
		}
	}
	mu.Unlock()

	// Verify total accumulated bytes matches file size
	if totalBytes != int64(len(testData)) {
		t.Errorf("Total accumulated bytes %d != expected %d", totalBytes, len(testData))
	}

	// Verify total read matches
	if totalRead != len(testData) {
		t.Errorf("Total read %d != expected %d", totalRead, len(testData))
	}
}

// BenchmarkCalcMd5FileWithProgress measures overhead of progress reporting
func BenchmarkCalcMd5FileWithProgress(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "checksum-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a 10MB file
	testFile := filepath.Join(tmpDir, "benchmark.bin")
	testData := bytes.Repeat([]byte("benchmark"), 1024*1024) // 9MB
	if err := os.WriteFile(testFile, testData, 0o644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CalcMd5FileWithProgress(tmpDir, "benchmark.bin", func(int64, time.Time) {})
		if err != nil {
			b.Fatalf("CalcMd5FileWithProgress failed: %v", err)
		}
	}
}

// BenchmarkCalcMd5File measures baseline performance without progress reporting
func BenchmarkCalcMd5File(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "checksum-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a 10MB file
	testFile := filepath.Join(tmpDir, "benchmark.bin")
	testData := bytes.Repeat([]byte("benchmark"), 1024*1024) // 9MB
	if err := os.WriteFile(testFile, testData, 0o644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CalcMd5File(tmpDir, "benchmark.bin")
		if err != nil {
			b.Fatalf("CalcMd5File failed: %v", err)
		}
	}
}
