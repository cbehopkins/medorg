package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestConfigLoader(t *testing.T) {
	// Create temp directory for test
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test.xml")

	var buf bytes.Buffer
	loader := NewConfigLoader(configPath, &buf)

	// Test Load
	xc, exitCode := loader.Load()
	if exitCode != ExitOk {
		t.Errorf("Expected ExitOk, got %d", exitCode)
	}
	if xc == nil {
		t.Error("Expected config to be created")
	}
}

func TestSourceDirResolver_Resolve(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test directories
	dir1 := filepath.Join(tmpDir, "dir1")
	dir2 := filepath.Join(tmpDir, "dir2")
	os.MkdirAll(dir1, 0o755)
	os.MkdirAll(dir2, 0o755)

	// Create config with source directories
	configPath := filepath.Join(tmpDir, "config.xml")
	xc, _ := core.NewMdConfig(configPath)
	xc.AddSourceDirectory(dir1, "test1")

	t.Run("CLI args take priority", func(t *testing.T) {
		var buf bytes.Buffer
		resolver := NewSourceDirResolver([]string{dir2}, xc, &buf)

		dirs, exitCode := resolver.Resolve()
		if exitCode != ExitOk {
			t.Errorf("Expected ExitOk, got %d", exitCode)
		}
		if len(dirs) != 1 || dirs[0] != dir2 {
			t.Errorf("Expected [%s], got %v", dir2, dirs)
		}
	})

	t.Run("Config used when no CLI args", func(t *testing.T) {
		var buf bytes.Buffer
		resolver := NewSourceDirResolver([]string{}, xc, &buf)

		dirs, exitCode := resolver.Resolve()
		if exitCode != ExitOk {
			t.Errorf("Expected ExitOk, got %d", exitCode)
		}
		if len(dirs) != 1 || dirs[0] != dir1 {
			t.Errorf("Expected [%s], got %v", dir1, dirs)
		}
	})

	t.Run("Current dir used when no config", func(t *testing.T) {
		var buf bytes.Buffer
		resolver := NewSourceDirResolver([]string{}, nil, &buf)

		dirs, exitCode := resolver.Resolve()
		if exitCode != ExitOk {
			t.Errorf("Expected ExitOk, got %d", exitCode)
		}
		if len(dirs) != 1 || dirs[0] != "." {
			t.Errorf("Expected [.], got %v", dirs)
		}
	})
}

func TestSourceDirResolver_ResolveWithValidation(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistentDir := filepath.Join(tmpDir, "does-not-exist")

	var buf bytes.Buffer
	resolver := NewSourceDirResolver([]string{nonExistentDir}, nil, &buf)

	dirs, exitCode := resolver.ResolveWithValidation()
	if exitCode != ExitSuppliedDirNotFound {
		t.Errorf("Expected ExitSuppliedDirNotFound, got %d", exitCode)
	}
	if dirs != nil {
		t.Errorf("Expected nil dirs for non-existent path, got %v", dirs)
	}
	if !bytes.Contains(buf.Bytes(), []byte("does not exist")) {
		t.Errorf("Expected error message about non-existent path, got: %s", buf.String())
	}
}

func TestValidatePath(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a file and a directory
	testFile := filepath.Join(tmpDir, "testfile.txt")
	testDir := filepath.Join(tmpDir, "testdir")
	os.WriteFile(testFile, []byte("test"), 0o644)
	os.MkdirAll(testDir, 0o755)

	t.Run("Existing file", func(t *testing.T) {
		err := ValidatePath(testFile, false)
		if err != nil {
			t.Errorf("Expected no error for existing file, got: %v", err)
		}
	})

	t.Run("Existing directory", func(t *testing.T) {
		err := ValidatePath(testDir, true)
		if err != nil {
			t.Errorf("Expected no error for existing directory, got: %v", err)
		}
	})

	t.Run("File when directory required", func(t *testing.T) {
		err := ValidatePath(testFile, true)
		if err == nil {
			t.Error("Expected error when file is not a directory")
		}
	})

	t.Run("Non-existent path", func(t *testing.T) {
		err := ValidatePath(filepath.Join(tmpDir, "nope"), false)
		if err == nil {
			t.Error("Expected error for non-existent path")
		}
	})
}
