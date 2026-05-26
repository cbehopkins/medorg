package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestCLI_ConfigPathOverride(t *testing.T) {
	tmpDir := t.TempDir()
	homeDir := filepath.Join(tmpDir, "home")
	sourceDir := filepath.Join(tmpDir, "source")
	configPath := filepath.Join(tmpDir, "override.mdcfg.xml")

	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		t.Fatalf("Failed to create home dir: %v", err)
	}
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "one.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("Failed to write source file: %v", err)
	}

	xc, err := core.LoadOrCreateMdConfigWithPath(configPath)
	if err != nil {
		t.Fatalf("Failed to load/create override config: %v", err)
	}
	if ok := xc.AddSourceDirectory(sourceDir, "test"); !ok {
		t.Fatalf("Failed to add source alias to override config")
	}
	if err := xc.WriteXmlCfg(); err != nil {
		t.Fatalf("Failed to write override config: %v", err)
	}

	cmd := exec.Command("go", "run", ".", "-config", configPath, sourceDir)
	cmd.Dir = filepath.Join(".")
	cmd.Env = append(os.Environ(),
		"HOME="+homeDir,
		"USERPROFILE="+homeDir,
		"HOMEDRIVE=",
		"HOMEPATH=",
		"GOFLAGS=-modcacherw",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdjournal command failed: %v\nOutput: %s", err, output)
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, "Loading config with path: "+configPath) {
		t.Fatalf("Expected mdjournal to load overridden config path %q, output: %s", configPath, outputStr)
	}
}
