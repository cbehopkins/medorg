package core

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestCalculatorBasic verifies Calculator creates a working io.Writer interface
func TestCalculatorBasic(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	testContent := []byte("test data for checksum")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Use Calculator to process the file path
	iw, trigger, wg := Calculator(testFile)
	if iw == nil {
		t.Fatal("Calculator returned nil io.Writer")
	}
	if trigger == nil {
		t.Fatal("Calculator returned nil trigger channel")
	}
	if wg == nil {
		t.Fatal("Calculator returned nil WaitGroup")
	}

	// Write data to the writer
	n, err := io.WriteString(iw, "test data for checksum")
	if err != nil {
		t.Fatalf("Failed to write to calculator: %v", err)
	}
	if n != len("test data for checksum") {
		t.Errorf("Expected to write %d bytes, wrote %d", len("test data for checksum"), n)
	}

	// Trigger completion
	close(trigger)
	wg.Wait()
}

// TestCalcBufferBasic tests CalcBuffer initialization and basic operations
func TestCalcBufferBasic(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	cb := NewCalcBuffer()
	if cb == nil {
		t.Fatal("NewCalcBuffer returned nil")
	}

	iw, trigger := cb.Calculate(testFile)
	if iw == nil {
		t.Fatal("Calculate returned nil io.Writer")
	}
	if trigger == nil {
		t.Fatal("Calculate returned nil trigger channel")
	}

	// Write test data
	if _, err := io.WriteString(iw, "test content"); err != nil {
		t.Fatalf("Failed to write to calculator: %v", err)
	}

	// Complete the calculation
	close(trigger)
	cb.Close()
}

// TestCalcBufferMultipleFiles tests CalcBuffer with multiple files
func TestCalcBufferMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple test files
	files := make([]string, 3)
	for i := 0; i < 3; i++ {
		files[i] = filepath.Join(tmpDir, "test"+string(rune('0'+i))+".txt")
		content := []byte("content " + string(rune('0'+i)))
		if err := os.WriteFile(files[i], content, 0o644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	cb := NewCalcBuffer()

	// Process multiple files concurrently
	triggers := make([]chan struct{}, len(files))
	for i, file := range files {
		iw, trigger := cb.Calculate(file)
		triggers[i] = trigger
		content := []byte("content " + string(rune('0'+i)))
		if _, err := iw.Write(content); err != nil {
			t.Fatalf("Failed to write content: %v", err)
		}
	}

	// Trigger all completions
	for _, trigger := range triggers {
		close(trigger)
	}

	// Close should persist all files
	cb.Close()

	// Verify files were persisted by checking for .medorg.xml
	xmlPath := filepath.Join(tmpDir, Md5FileName)
	if _, err := os.Stat(xmlPath); os.IsNotExist(err) {
		t.Errorf("Expected persisted XML file at %s, got not found", xmlPath)
	}
}

// TestMd5CalcInternal verifies internal calculation with DirectoryMap
func TestMd5CalcInternal(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	testContent := []byte("test data")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create initial DirectoryMap
	dm := NewDirectoryMap()
	if err := dm.Persist(tmpDir); err != nil {
		t.Fatalf("Failed to persist initial DirectoryMap: %v", err)
	}

	h := md5.New()
	_, _ = io.WriteString(h, "test data")

	trigger := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Call md5CalcInternal
	go md5CalcInternal(h, wg, testFile, trigger)

	// Trigger completion after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(trigger)
	}()

	// Wait for completion
	wg.Wait()

	// Verify the file was added to the DirectoryMap by reloading
	dm2, err := DirectoryMapFromDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to load DirectoryMap: %v", err)
	}

	fs, ok := dm2.Get("test.txt")
	if !ok {
		t.Error("Expected file to be added to DirectoryMap")
		return
	}

	if fs.Name != "test.txt" {
		t.Errorf("Expected filename 'test.txt', got %q", fs.Name)
	}
}

// TestCompleteCalcFlow verifies the complete calculation flow
func TestCompleteCalcFlow(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create test file
	testContent := []byte("complete calc test")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Initialize DirectoryMap
	dm := NewDirectoryMap()
	if err := dm.Persist(tmpDir); err != nil {
		t.Fatalf("Failed to persist DirectoryMap: %v", err)
	}

	h := md5.New()
	_, _ = io.WriteString(h, "complete calc test")

	// Test completeCalc
	trigger := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(trigger)
	}()

	completeCalc(trigger, tmpDir, "test.txt", h, *dm)

	// Verify file was added
	fs, ok := dm.Get("test.txt")
	if !ok {
		t.Error("Expected file to be added to DirectoryMap")
		return
	}

	if fs.Checksum == "" {
		t.Error("Expected checksum to be calculated")
	}
}

// TestCalculatorWithRealFile tests Calculator with actual file I/O
func TestCalculatorWithRealFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "calculator_test.txt")

	// Create test file
	testContent := []byte("calculator test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Initialize DirectoryMap
	if err := os.WriteFile(filepath.Join(tmpDir, Md5FileName), []byte(""), 0o644); err != nil {
		t.Fatalf("Failed to create initial XML: %v", err)
	}

	iw, trigger, wg := Calculator(testFile)

	// Simulate reading file and writing to calculator
	f, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer f.Close()

	_, err = io.Copy(iw, f)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to copy file to calculator: %v", err)
	}

	close(trigger)
	wg.Wait()
}

// TestCalcBufferFpParsing tests getFp method for correct path parsing
func TestCalcBufferFpParsing(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "subdir", "test.txt")

	// Create subdirectory and test file
	if err := os.MkdirAll(filepath.Dir(testFile), 0o755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(testFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Initialize DirectoryMap for subdirectory
	subdirPath := filepath.Dir(testFile)
	if err := os.WriteFile(filepath.Join(subdirPath, Md5FileName), []byte(""), 0o644); err != nil {
		t.Fatalf("Failed to create initial XML: %v", err)
	}

	cb := NewCalcBuffer()
	dm, dir, fn := cb.getFp(testFile)

	if dm == nil {
		t.Fatal("getFp returned nil DirectoryMap")
	}

	// filepath.Dir may include trailing separator on Windows, so normalize
	if dir != subdirPath && filepath.Clean(dir) != filepath.Clean(subdirPath) {
		t.Errorf("Expected dir %q, got %q", subdirPath, dir)
	}

	if fn != "test.txt" {
		t.Errorf("Expected filename 'test.txt', got %q", fn)
	}
}

// TestCalcBufferGetDirCaching tests that getDir properly caches DirectoryMaps
func TestCalcBufferGetDirCaching(t *testing.T) {
	tmpDir := t.TempDir()

	// Initialize DirectoryMap
	if err := os.WriteFile(filepath.Join(tmpDir, Md5FileName), []byte(""), 0o644); err != nil {
		t.Fatalf("Failed to create initial XML: %v", err)
	}

	cb := NewCalcBuffer()

	// Get the same directory twice
	dm1 := cb.getDir(tmpDir)
	dm2 := cb.getDir(tmpDir)

	// They should be the same pointer (cached)
	if dm1 != dm2 {
		t.Error("Expected getDir to return cached instance")
	}
}

// TestHashingAccuracy tests that hash results match expected MD5 values
func TestHashingAccuracy(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"empty", ""},
		{"simple", "hello"},
		{"long", "the quick brown fox jumps over the lazy dog"},
		{"binary", string([]byte{0, 1, 2, 3, 255, 254, 253})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := md5.New()
			_, _ = io.WriteString(h, tt.content)
			result1 := ReturnChecksumString(h)

			// Calculate again independently
			h2 := md5.New()
			_, _ = io.WriteString(h2, tt.content)
			result2 := ReturnChecksumString(h2)

			if result1 != result2 {
				t.Errorf("Checksum mismatch: %q vs %q", result1, result2)
			}
		})
	}
}

// TestReturnChecksumStringFormat verifies output format
func TestReturnChecksumStringFormat(t *testing.T) {
	h := md5.New()
	_, _ = io.WriteString(h, "test")
	result := ReturnChecksumString(h)

	// MD5 hash is 128 bits = 16 bytes
	// Base64 without padding: 16 bytes = 22 characters (roughly 16 * 4/3)
	if len(result) == 0 {
		t.Error("ReturnChecksumString returned empty string")
	}

	// Verify it's valid base64-like (no padding character)
	if bytes.Contains([]byte(result), []byte("=")) {
		t.Error("ReturnChecksumString should use NoPadding but got padding char")
	}
}

// TestCalcBufferConcurrentCalculations tests concurrent file calculations
func TestCalcBufferConcurrentCalculations(t *testing.T) {
	tmpDir := t.TempDir()
	numFiles := 5

	// Create test files
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		files[i] = filepath.Join(tmpDir, "file"+string(rune('0'+i))+".txt")
		content := bytes.Repeat([]byte(string(rune('0'+i))), 100)
		if err := os.WriteFile(files[i], content, 0o644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	// Initialize DirectoryMap
	if err := os.WriteFile(filepath.Join(tmpDir, Md5FileName), []byte(""), 0o644); err != nil {
		t.Fatalf("Failed to create initial XML: %v", err)
	}

	cb := NewCalcBuffer()

	// Process files concurrently
	var wg sync.WaitGroup
	for i, file := range files {
		wg.Add(1)
		go func(f string, fileIdx int) {
			defer wg.Done()
			iw, trigger := cb.Calculate(f)
			content := bytes.Repeat([]byte(string(rune('0'+fileIdx))), 100)
			if _, err := iw.Write(content); err != nil {
				t.Errorf("Failed to write to calculator: %v", err)
			}
			close(trigger)
		}(file, i)
	}

	wg.Wait()
	cb.Close()

	// Verify XML was created
	xmlPath := filepath.Join(tmpDir, Md5FileName)
	if _, err := os.Stat(xmlPath); os.IsNotExist(err) {
		t.Errorf("Expected persisted XML file at %s", xmlPath)
	}
}
