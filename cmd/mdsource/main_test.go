package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
)

// helper to create temp config path
func tempConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "config.xml")
}

func TestMdsource_AddListRemove_RestoreDest(t *testing.T) {
	cfgPath := tempConfig(t)

	// Run: add
	var out bytes.Buffer
	srcDir := filepath.Join(t.TempDir(), "srcA")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("failed to create src dir: %v", err)
	}
	os.Args = []string{"mdsource", "add", "--path", srcDir, "--alias", "media", "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("add exit code=%d output=%s", code, out.String())
	}

	// Run: list
	out.Reset()
	os.Args = []string{"mdsource", "list", "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("list exit code=%d output=%s", code, out.String())
	}

	// Run: restore set
	out.Reset()
	destDir := filepath.Join(t.TempDir(), "restoreA")
	os.Args = []string{"mdsource", "restore", "--alias", "media", "--path", destDir, "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("restore set exit code=%d output=%s", code, out.String())
	}

	// Run: remove
	out.Reset()
	os.Args = []string{"mdsource", "remove", "--alias", "media", "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("remove exit code=%d output=%s", code, out.String())
	}
}

func TestMdsource_InvalidAliasAndPath(t *testing.T) {
	cfgPath := tempConfig(t)

	// remove non-existent
	var out bytes.Buffer
	os.Args = []string{"mdsource", "remove", "--alias", "missing", "--config", cfgPath}
	if code := run(&out); code != cli.ExitAliasNotFound {
		t.Fatalf("expected ExitAliasNotFound got %d output=%s", code, out.String())
	}

	// add with non-existent path
	out.Reset()
	os.Args = []string{"mdsource", "add", "--path", "Z:/does/not/exist", "--alias", "bad", "--config", cfgPath}
	if code := run(&out); code != cli.ExitPathNotExist {
		t.Fatalf("expected ExitPathNotExist got %d output=%s", code, out.String())
	}
}

func TestMdsource_IgnoreAddAndTest(t *testing.T) {
	cfgPath := tempConfig(t)

	var out bytes.Buffer

	// add ignore pattern
	os.Args = []string{"mdsource", "ignore-add", "--pattern", "Recycle Bin", "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("ignore-add exit code=%d output=%s", code, out.String())
	}

	// test ignored path
	out.Reset()
	os.Args = []string{"mdsource", "ignore-test", "--path", "home/Recycle Bin/bob/my.txt", "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("ignore-test exit code=%d output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "IGNORED") {
		t.Fatalf("expected path to be ignored, output=%s", out.String())
	}

	// test non-ignored path
	out.Reset()
	os.Args = []string{"mdsource", "ignore-test", "--path", "home/keep.txt", "--config", cfgPath}
	if code := run(&out); code != cli.ExitOk {
		t.Fatalf("ignore-test exit code=%d output=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "NOT IGNORED") {
		t.Fatalf("expected path not to be ignored, output=%s", out.String())
	}
}
