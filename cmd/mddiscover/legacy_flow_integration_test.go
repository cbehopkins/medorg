package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func countFilesForIntegrationTest(dir string) int {
	count := 0
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && info.Name() != ".medorg.xml" {
			count++
		}
		return nil
	})
	return count
}

func TestMddiscoverLegacyDestinationFlow(t *testing.T) {
	tmpRoot, err := os.MkdirTemp("", "mddiscover-legacy-flow-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	sourceDir := filepath.Join(tmpRoot, "source")
	destDir := filepath.Join(tmpRoot, "legacy-dest")
	configFile := filepath.Join(tmpRoot, "test.mdcfg.xml")

	for _, dir := range []string{sourceDir, destDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
	}

	files := map[string]string{
		"photos/a.jpg": "legacy-photo-a",
		"docs/b.txt":   "legacy-doc-b",
	}
	for rel, content := range files {
		srcPath := filepath.Join(sourceDir, rel)
		dstPath := filepath.Join(destDir, rel)
		if err := os.MkdirAll(filepath.Dir(srcPath), 0o755); err != nil {
			t.Fatalf("Failed creating src parent dir: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			t.Fatalf("Failed creating dst parent dir: %v", err)
		}
		if err := os.WriteFile(srcPath, []byte(content), 0o644); err != nil {
			t.Fatalf("Failed writing source file: %v", err)
		}
		if err := os.WriteFile(dstPath, []byte(content), 0o644); err != nil {
			t.Fatalf("Failed writing destination file: %v", err)
		}
	}

	buildBinariesForIntegrationTest(t, tmpRoot, []string{"mdlabel", "mdsource", "mddiscover", "mdbackup"})
	mdlabelBin := filepath.Join(tmpRoot, "mdlabel.exe")
	mdsourceBin := filepath.Join(tmpRoot, "mdsource.exe")
	mddiscoverBin := filepath.Join(tmpRoot, "mddiscover.exe")
	mdbackupBin := filepath.Join(tmpRoot, "mdbackup.exe")

	// Label legacy destination so discovery can map by volume label.
	cmd := exec.Command(mdlabelBin, "create", destDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdlabel create failed: %v\nOutput: %s", err, output)
	}

	// Configure source in config for mddiscover.
	cmd = exec.Command(mdsourceBin, "add", "-config", configFile, "-path", sourceDir, "-alias", "legacy")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdsource add failed: %v\nOutput: %s", err, output)
	}

	// Run discovery against legacy destination.
	cmd = exec.Command(mddiscoverBin, "-config", configFile, destDir)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mddiscover failed: %v\nOutput: %s", err, output)
	}
	outStr := string(output)
	if !strings.Contains(outStr, "Files matched:") || !strings.Contains(outStr, "Metadata updated:") {
		t.Fatalf("Unexpected mddiscover output: %s", outStr)
	}

	xc := &core.MdConfig{}
	volumeLabel, err := xc.GetVolumeLabel(destDir)
	if err != nil {
		t.Fatalf("Failed to read destination label: %v", err)
	}

	for rel := range files {
		dir := core.Dirname(filepath.Dir(filepath.Join(sourceDir, rel)))
		name := core.Fname(filepath.Base(rel))
		dm, err := core.DirectoryMapFromDir(dir, nil)
		if err != nil {
			t.Fatalf("Failed to load source metadata for %s: %v", rel, err)
		}
		fs, ok := dm.Get(name)
		if !ok {
			t.Fatalf("Missing metadata entry for %s", rel)
		}
		if !fs.HasTag(volumeLabel) {
			t.Fatalf("Expected %s to be tagged with discovered destination label %q, tags=%v", rel, volumeLabel, fs.BackupDest)
		}
	}

	beforeCount := countFilesForIntegrationTest(destDir)

	// After discovery, backup should find content already present and not need data copies.
	cmd = exec.Command(mdbackupBin, destDir, sourceDir)
	cmd.Env = append(os.Environ(), "MEDORG_NO_PROGRESS=1", "NO_COLOR=1", "TERM=dumb")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mdbackup after discovery failed: %v\nOutput: %s", err, output)
	}

	afterCount := countFilesForIntegrationTest(destDir)
	if afterCount != beforeCount {
		t.Fatalf("Expected no additional destination files after discovery-guided backup: before=%d after=%d", beforeCount, afterCount)
	}
}
