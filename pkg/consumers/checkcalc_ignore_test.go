package consumers

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestRunCheckCalcSkipsIgnoredDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	keepDir := filepath.Join(root, "keep")
	binDir := filepath.Join(root, "Recycle Bin", "nested")
	for _, d := range []string{keepDir, binDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
		if err := os.WriteFile(filepath.Join(d, "a.txt"), []byte("data"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}

	re := regexp.MustCompile(`(?i)[/\\]Recycle Bin([/\\]|$)`)
	opts := CheckCalcOptions{CalcCount: 1, Ignore: re.MatchString}
	if err := RunCheckCalc([]string{root}, opts); err != nil {
		t.Fatalf("RunCheckCalc failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(keepDir, core.Md5FileName)); err != nil {
		t.Fatalf("expected metadata in non-ignored dir: %v", err)
	}
	for _, d := range []string{filepath.Join(root, "Recycle Bin"), binDir} {
		_, err := os.Stat(filepath.Join(d, core.Md5FileName))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected no metadata in ignored dir %s, stat err=%v", d, err)
		}
	}
}
