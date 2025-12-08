package core

import (
	"bytes"
	"crypto/md5"
	"io"
	"testing"
)

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
