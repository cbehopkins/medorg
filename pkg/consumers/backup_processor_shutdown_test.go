package consumers

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cbehopkins/medorg/pkg/core"
)

// TestBackupProcessorGracefulShutdown tests that closing BackupProcessor
// while operations are in progress doesn't panic with "send on closed channel"
func TestBackupProcessorGracefulShutdown(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}

	// Start multiple concurrent operations
	const numWorkers = 20
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// Generate some MD5 keys and try to add files
			for j := 0; j < 100; j++ {
				// Create a simple hex MD5 key string
				md5Str := string([]byte{
					byte(workerID), byte(workerID), byte(j), byte(j),
					byte(workerID), byte(workerID), byte(j), byte(j),
					byte(workerID), byte(workerID), byte(j), byte(j),
					byte(workerID), byte(workerID), byte(j), byte(j),
				})
				// Convert to hex string (32 chars)
				md5Hex := ""
				for _, b := range []byte(md5Str) {
					md5Hex += string("0123456789abcdef"[b>>4]) + string("0123456789abcdef"[b&0xf])
				}
				
				file := core.NewFpath("/test", "file.txt")
				err := bp.addSrcFile(md5Hex, 1234, []string{}, file)
				if err != nil {
					// We expect some errors when shutting down
					if !strings.Contains(err.Error(), "shutting down") {
						errors <- err
					}
					return
				}
				
				// Small delay to increase chance of hitting shutdown
				time.Sleep(time.Microsecond * 10)
			}
		}(i)
	}

	// Close the BackupProcessor after a short delay while workers are still running
	time.Sleep(time.Millisecond * 50)
	closeErr := bp.Close()
	if closeErr != nil {
		t.Errorf("Close() returned error: %v", closeErr)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(errors)

	// Check for unexpected errors (not shutdown errors)
	for err := range errors {
		t.Errorf("Unexpected error: %v", err)
	}

	// If we get here without panic, the test passed
	t.Log("Graceful shutdown completed successfully")
}

// TestBackupProcessorDoubleClose tests that calling Close twice doesn't panic
func TestBackupProcessorDoubleClose(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}

	// First close
	if err := bp.Close(); err != nil {
		t.Errorf("First Close() returned error: %v", err)
	}

	// Second close - should not panic but may return error
	err = bp.Close()
	// We don't fail the test if second close returns an error,
	// but we do fail if it panics (test will stop)
	t.Logf("Second Close() returned: %v", err)
}

// TestBackupProcessorCloseIdempotent tests Close is safe to call multiple times concurrently
func TestBackupProcessorCloseIdempotent(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}

	// Call Close from multiple goroutines simultaneously
	const numClosers = 10
	var wg sync.WaitGroup
	errors := make(chan error, numClosers)

	for i := 0; i < numClosers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bp.Close(); err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Logf("Close error: %v", err)
		errorCount++
	}

	// Some errors are acceptable, but no panics
	t.Logf("Total close errors: %d out of %d", errorCount, numClosers)
}

// TestBackupProcessorOperationAfterClose verifies operations after Close return errors
func TestBackupProcessorOperationAfterClose(t *testing.T) {
	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}

	// Close first
	if err := bp.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Try to add a file after close
	md5Hex := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
	file := core.NewFpath("/test", "file.txt")
	
	err = bp.addSrcFile(md5Hex, 1234, []string{}, file)
	if err == nil {
		t.Error("Expected error when adding file after Close, got nil")
	} else if !strings.Contains(err.Error(), "shutting down") {
		t.Errorf("Expected 'shutting down' error, got: %v", err)
	} else {
		t.Logf("Correctly returned error: %v", err)
	}
}

// TestBackupProcessorHighConcurrency stress tests with many concurrent operations
func TestBackupProcessorHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	bp, err := NewBackupProcessor()
	if err != nil {
		t.Fatalf("Failed to create BackupProcessor: %v", err)
	}
	defer bp.Close()

	const numWorkers = 50
	const opsPerWorker = 100
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*opsPerWorker)

	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < opsPerWorker; j++ {
				// Create a simple hex MD5 key string  
				md5Str := string([]byte{
					byte(workerID >> 8), byte(workerID), byte(j), byte(j),
					byte(workerID >> 8), byte(workerID), byte(j), byte(j),
					byte(workerID >> 8), byte(workerID), byte(j), byte(j),
					byte(workerID >> 8), byte(workerID), byte(j), byte(j),
				})
				// Convert to hex string (32 chars)
				md5Hex := ""
				for _, b := range []byte(md5Str) {
					md5Hex += string("0123456789abcdef"[b>>4]) + string("0123456789abcdef"[b&0xf])
				}
				
				file := core.NewFpath("/test", "file.txt")
				if err := bp.addSrcFile(md5Hex, int64(j), []string{}, file); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	duration := time.Since(startTime)
	
	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Operation error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Failed with %d errors out of %d operations", errorCount, numWorkers*opsPerWorker)
	}

	t.Logf("Completed %d operations in %v (%d ops/sec)",
		numWorkers*opsPerWorker,
		duration,
		int64(float64(numWorkers*opsPerWorker)/duration.Seconds()))
}
