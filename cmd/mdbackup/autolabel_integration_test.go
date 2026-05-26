package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func buildBinariesForIntegrationTest(t *testing.T, tmpRoot string, binaries []string) {
	t.Helper()
	for _, binary := range binaries {
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", filepath.Join(tmpRoot, binary+".exe"), "./cmd/"+binary)
		cmd.Dir = filepath.Join("..", "..")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Failed to build %s: %v\nOutput: %s", binary, err, output)
		}
	}
}

func TestMdbackupAutoCreatesDestinationLabel(t *testing.T) {
	tmpRoot, err := os.MkdirTemp("", "mdbackup-autolabel-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	sourceDir := filepath.Join(tmpRoot, "source")
	destDir := filepath.Join(tmpRoot, "dest")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("Failed to create dest dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(sourceDir, "auto-label-test.txt"), []byte("auto label content"), 0o644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	buildBinariesForIntegrationTest(t, tmpRoot, []string{"mdbackup"})
	mdbackupBin := filepath.Join(tmpRoot, "mdbackup.exe")

	labelPath := filepath.Join(destDir, ".mdbackup.xml")
	if _, err := os.Stat(labelPath); !os.IsNotExist(err) {
		t.Fatalf("Destination should start unlabeled for this test")
	}

	cmd := exec.Command(mdbackupBin, destDir, sourceDir)
	cmd.Env = append(os.Environ(), "MEDORG_NO_PROGRESS=1", "NO_COLOR=1", "TERM=dumb")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdbackup failed: %v\nOutput: %s", err, output)
	}

	if _, err := os.Stat(labelPath); err != nil {
		t.Fatalf("Expected mdbackup to auto-create %s: %v", labelPath, err)
	}

	xc := &core.MdConfig{}
	volumeLabel, err := xc.GetVolumeLabel(destDir)
	if err != nil {
		t.Fatalf("Failed to read destination volume label: %v", err)
	}
	if volumeLabel == "" {
		t.Fatalf("Expected non-empty volume label after mdbackup")
	}

	dm, err := core.DirectoryMapFromDir(core.Dirname(sourceDir), nil)
	if err != nil {
		t.Fatalf("Failed to read source directory metadata: %v", err)
	}
	fs, ok := dm.Get(core.Fname("auto-label-test.txt"))
	if !ok {
		t.Fatalf("Source metadata entry missing for auto-label-test.txt")
	}
	if !fs.HasTag(volumeLabel) {
		t.Fatalf("Expected source file to be tagged with auto-created volume label %q, tags=%v", volumeLabel, fs.BackupDest)
	}
}
