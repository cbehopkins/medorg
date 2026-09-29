package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

func TestResolveDirectoriesForRun_WithScanPathsInConfig(t *testing.T) {
	tempDir := t.TempDir()
	scanDir := filepath.Join(tempDir, "photos")
	if err := os.MkdirAll(scanDir, 0o755); err != nil {
		t.Fatalf("Failed to create scan dir: %v", err)
	}

	xc := &core.MdConfig{
		SourceDirectories: []core.SourceDirectory{{Path: filepath.Clean(scanDir), Alias: "photos"}},
	}

	got, exitCode := resolveDirectoriesForRun([]string{scanDir}, nil, xc, &bytes.Buffer{})
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected ExitOk, got %d", exitCode)
	}
	if len(got) != 1 || got[0] != scanDir {
		t.Fatalf("Unexpected directories result: %#v", got)
	}
}

func TestResolveDirectoriesForRun_WithScanPathMissingFromConfig(t *testing.T) {
	tempDir := t.TempDir()
	scanDir := filepath.Join(tempDir, "videos")
	var stdout bytes.Buffer

	xc := &core.MdConfig{SourceDirectories: []core.SourceDirectory{}}

	_, exitCode := resolveDirectoriesForRun([]string{scanDir}, nil, xc, &stdout)
	if exitCode != cli.ExitConfigError {
		t.Fatalf("Expected ExitConfigError, got %d", exitCode)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("not configured")) {
		t.Fatalf("Expected config error message, got: %s", stdout.String())
	}
}

func TestResolveDirectoriesForRun_FallsBackToArgs(t *testing.T) {
	tempDir := t.TempDir()
	dirA := filepath.Join(tempDir, "music")
	if err := os.MkdirAll(dirA, 0o755); err != nil {
		t.Fatalf("Failed to create test dir: %v", err)
	}

	got, exitCode := resolveDirectoriesForRun(nil, []string{dirA}, &core.MdConfig{}, &bytes.Buffer{})
	if exitCode != cli.ExitOk {
		t.Fatalf("Expected ExitOk, got %d", exitCode)
	}
	if len(got) != 1 || got[0] != dirA {
		t.Fatalf("Unexpected directories result: %#v", got)
	}
}
