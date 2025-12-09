package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Integration tests for mdbackup

// Helper function to setup volume configs for source and destination directories
func setupVolumeConfigs(t *testing.T, xc *core.MdConfig, dirs ...string) {
	t.Helper()
	for _, dir := range dirs {
		vc, err := xc.VolumeCfgFromDir(dir)
		if err != nil {
			t.Fatalf("Failed to get volume config for %s: %v", dir, err)
		}
		if err := vc.Persist(); err != nil {
			t.Fatalf("Failed to persist volume config for %s: %v", dir, err)
		}
	}
}

// helper: create temp dirs; returns paths and a cleanup func
func makeTempDirs(t *testing.T, names ...string) (map[string]string, func()) {
	t.Helper()
	paths := make(map[string]string, len(names))
	var toClean []string
	for _, n := range names {
		d, err := os.MkdirTemp("", n+"-*")
		if err != nil {
			t.Fatalf("failed to create temp dir %s: %v", n, err)
		}
		paths[n] = d
		toClean = append(toClean, d)
	}
	cleanup := func() {
		for _, d := range toClean {
			_ = os.RemoveAll(d)
		}
	}
	return paths, cleanup
}

// helper: create XML config at given base directory
func newXMLCfgAt(t *testing.T, base string) *core.MdConfig {
	t.Helper()
	cfgFile := filepath.Join(base, ".medorg.xml")
	xc, err := core.NewMdConfig(cfgFile)
	if err != nil {
		t.Fatalf("Failed to create XML config: %v", err)
	}
	return xc
}

func TestIntegration_TagMode(t *testing.T) {
	cases := []struct {
		name       string
		withSource bool
		checkLabel bool
	}{
		{name: "DestinationOnly_LabelPrinted", withSource: false, checkLabel: true},
		{name: "DestinationWithExtraSources", withSource: true, checkLabel: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dirs, cleanup := makeTempDirs(t, "tag-dest", "tag-src")
			defer cleanup()

			xc := newXMLCfgAt(t, dirs["tag-dest"])
			// persist volume for destination
			vc, err := xc.VolumeCfgFromDir(dirs["tag-dest"])
			if err != nil {
				t.Fatalf("Failed to get volume config: %v", err)
			}
			if err := vc.Persist(); err != nil {
				t.Fatalf("Failed to persist volume config: %v", err)
			}
			label := vc.Label

			var logBuf, msgBuf bytes.Buffer
			cfg := Config{
				Destination:          dirs["tag-dest"],
				VolumeConfigProvider: xc,
				TagMode:              true,
				LogOutput:            &logBuf,
				MessageWriter:        &msgBuf,
				UseProgressBar:       false,
			}
			if tc.withSource {
				cfg.Sources = []string{dirs["tag-src"]}
			}

			exitCode, err := Run(cfg)
			if exitCode != cli.ExitOk || err != nil {
				t.Fatalf("Tag mode failed: exit=%d err=%v", exitCode, err)
			}
			if tc.checkLabel {
				if !strings.Contains(msgBuf.String(), label) {
					t.Errorf("Expected output to contain label %q, got: %s", label, msgBuf.String())
				}
			}
		})
	}
}

func TestIntegration_BasicBackup_NoFiles(t *testing.T) {
	// Create source and destination directories
	srcDir, err := os.MkdirTemp("", "mdbackup-src-*")
	if err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}
	defer os.RemoveAll(srcDir)

	dstDir, err := os.MkdirTemp("", "mdbackup-dst-*")
	if err != nil {
		t.Fatalf("Failed to create destination directory: %v", err)
	}
	defer os.RemoveAll(dstDir)

	// Create XML config
	configFile := filepath.Join(srcDir, core.ConfigFileName)
	xc, err := core.NewMdConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to create XML config: %v", err)
	}

	// Add volume configurations
	setupVolumeConfigs(t, xc, srcDir, dstDir)

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		Destination:          dstDir,
		Sources:              []string{srcDir},
		VolumeConfigProvider: xc,
		DummyMode:            true, // Don't actually copy in this test
		LogOutput:            &logBuf,
		MessageWriter:        &msgBuf,
		UseProgressBar:       false,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify completion message
	output := msgBuf.String()
	if !strings.Contains(output, "Completed Backup Run") {
		t.Errorf("Expected completion message, got: %s", output)
	}
}

func TestIntegration_BackupCopyVariants(t *testing.T) {
	type fileSpec struct {
		srcIndex int
		relPath  string
		content  string
	}

	cases := []struct {
		name    string
		sources int
		files   []fileSpec
	}{
		{
			name:    "SingleSource_SingleFile",
			sources: 1,
			files:   []fileSpec{{0, "test.txt", "test content for backup"}},
		},
		{
			name:    "SingleSource_MultipleFiles",
			sources: 1,
			files:   []fileSpec{{0, "file1.txt", "content 1"}, {0, "file2.dat", "content 2"}, {0, "file3.log", "content 3"}},
		},
		{
			name:    "SingleSource_Nested",
			sources: 1,
			files:   []fileSpec{{0, "root.txt", "root content"}, {0, filepath.Join("subdir", "level1.txt"), "level 1 content"}, {0, filepath.Join("subdir", "nested", "level2.txt"), "level 2 content"}},
		},
		{
			name:    "MultiSource_Flat",
			sources: 2,
			files:   []fileSpec{{0, "a1.txt", "alpha one"}, {0, "b1.log", "beta one"}, {1, "a2.txt", "alpha two"}, {1, "b2.log", "beta two"}},
		},
		{
			name:    "MultiSource_Nested",
			sources: 2,
			files: []fileSpec{
				{0, "root1.txt", "r1"},
				{0, filepath.Join("dirA", "a.txt"), "a"},
				{0, filepath.Join("dirA", "nest", "deep1.txt"), "d1"},
				{1, "root2.txt", "r2"},
				{1, filepath.Join("dirB", "b.txt"), "b"},
				{1, filepath.Join("dirB", "nest", "deep2.txt"), "d2"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// create sources and destination
			names := []string{"dst"}
			for i := 0; i < tc.sources; i++ {
				names = append(names, fmt.Sprintf("src%d", i))
			}
			dirs, cleanup := makeTempDirs(t, names...)
			defer cleanup()

			// lay down files
			for _, fspec := range tc.files {
				src := dirs[fmt.Sprintf("src%d", fspec.srcIndex)]
				full := filepath.Join(src, fspec.relPath)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatalf("mkdirs: %v", err)
				}
				if err := os.WriteFile(full, []byte(fspec.content), 0o644); err != nil {
					t.Fatalf("write %s: %v", full, err)
				}
			}

			// config and labels
			xc := newXMLCfgAt(t, dirs["dst"])
			var srcPaths []string
			for i := 0; i < tc.sources; i++ {
				srcPaths = append(srcPaths, dirs[fmt.Sprintf("src%d", i)])
			}
			setupVolumeConfigs(t, xc, append(srcPaths, dirs["dst"])...)

			var logBuf, msgBuf bytes.Buffer
			cfg := Config{Destination: dirs["dst"], Sources: srcPaths, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
			exitCode, err := Run(cfg)
			if exitCode != cli.ExitOk || err != nil {
				t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
			}

			// verify files exist with correct content
			for _, fspec := range tc.files {
				dstPath := filepath.Join(dirs["dst"], fspec.relPath)
				b, err := os.ReadFile(dstPath)
				if err != nil {
					t.Errorf("missing expected file: %s", dstPath)
					continue
				}
				if string(b) != fspec.content {
					t.Errorf("content mismatch for %s: want %q got %q", dstPath, fspec.content, b)
				}
			}
		})
	}
}

func TestIntegration_DeleteMode(t *testing.T) {
	t.Run("SingleSource_SimpleOrphan", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// source keep, dest orphan
		if err := os.WriteFile(filepath.Join(dirs["src"], "kept.txt"), []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
		orphan := filepath.Join(dirs["dst"], "orphan.txt")
		if err := os.WriteFile(orphan, []byte("orphan"), 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"]) // keep config with destination
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, DeleteMode: true, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}
		if _, err := os.Stat(orphan); !os.IsNotExist(err) {
			t.Errorf("orphan not deleted: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dirs["dst"], "kept.txt")); os.IsNotExist(err) {
			t.Errorf("kept.txt missing")
		}
	})

	t.Run("MultiSource_NestedOrphans", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src1", "src2", "dst")
		defer cleanup()

		// seed sources
		if err := os.MkdirAll(filepath.Join(dirs["src1"], "unique1", "sub"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dirs["src1"], "unique1", "sub", "file1.txt"), []byte("source1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(dirs["src2"], "unique2", "sub"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dirs["src2"], "unique2", "sub", "file2.txt"), []byte("source2"), 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src1"], dirs["src2"], dirs["dst"])

		// initial backup
		var lb1, mb1 bytes.Buffer
		cfg1 := Config{Destination: dirs["dst"], Sources: []string{dirs["src1"], dirs["src2"]}, VolumeConfigProvider: xc, LogOutput: &lb1, MessageWriter: &mb1}
		if exitCode, err := Run(cfg1); exitCode != cli.ExitOk || err != nil {
			t.Fatalf("initial backup failed: exit=%d err=%v", exitCode, err)
		}

		// add orphans
		orphanRoot := filepath.Join(dirs["dst"], "orphan_root.txt")
		orphanUnique1 := filepath.Join(dirs["dst"], "unique1", "orphan.txt")
		orphanDeep1 := filepath.Join(dirs["dst"], "unique1", "sub", "orphan.txt")
		orphanUnique2 := filepath.Join(dirs["dst"], "unique2", "orphan.txt")
		_ = os.WriteFile(orphanRoot, []byte("orphan at root"), 0o644)
		_ = os.WriteFile(orphanUnique1, []byte("orphan in unique1"), 0o644)
		_ = os.WriteFile(orphanDeep1, []byte("orphan deep in unique1/sub"), 0o644)
		_ = os.WriteFile(orphanUnique2, []byte("orphan in unique2"), 0o644)

		// delete mode
		var lb2, mb2 bytes.Buffer
		cfg2 := Config{Destination: dirs["dst"], Sources: []string{dirs["src1"], dirs["src2"]}, VolumeConfigProvider: xc, DeleteMode: true, LogOutput: &lb2, MessageWriter: &mb2}
		if exitCode, err := Run(cfg2); exitCode != cli.ExitOk || err != nil {
			t.Fatalf("delete mode failed: exit=%d err=%v\nLog:\n%s", exitCode, err, lb2.String())
		}

		// verify orphans removed and valids present
		for _, p := range []string{orphanRoot, orphanUnique1, orphanDeep1, orphanUnique2} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Errorf("orphan not deleted: %s", p)
			}
		}
		for _, p := range []string{filepath.Join(dirs["dst"], "unique1", "sub", "file1.txt"), filepath.Join(dirs["dst"], "unique2", "sub", "file2.txt")} {
			if _, err := os.Stat(p); os.IsNotExist(err) {
				t.Errorf("valid file missing: %s", p)
			}
		}
	})
}

func TestIntegration_ScanMode(t *testing.T) {
	srcDir, _ := os.MkdirTemp("", "mdbackup-src-*")
	defer os.RemoveAll(srcDir)
	dstDir, _ := os.MkdirTemp("", "mdbackup-dst-*")
	defer os.RemoveAll(dstDir)

	// Create a test file
	testFile := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create XML config
	configFile := filepath.Join(srcDir, ".medorg.xml")
	xc, _ := core.NewMdConfig(configFile)
	setupVolumeConfigs(t, xc, srcDir, dstDir)

	var logBuf, msgBuf bytes.Buffer
	cfg := Config{
		Destination:          dstDir,
		Sources:              []string{srcDir},
		VolumeConfigProvider: xc,
		ScanMode:             true, // Scan only, don't copy
		LogOutput:            &logBuf,
		MessageWriter:        &msgBuf,
		UseProgressBar:       false,
	}

	exitCode, err := Run(cfg)
	if exitCode != cli.ExitOk {
		t.Errorf("Expected exit code %d, got %d", cli.ExitOk, exitCode)
	}
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify the file was NOT copied (scan mode)
	copiedFile := filepath.Join(dstDir, "test.txt")
	if _, err := os.Stat(copiedFile); !os.IsNotExist(err) {
		t.Error("File should not be copied in scan mode")
	}
}

func TestIntegration_ErrorCases(t *testing.T) {
	t.Run("InvalidDirectoryCount", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "base")
		defer cleanup()
		xc := newXMLCfgAt(t, dirs["base"])
		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["base"], VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitTwoDirectoriesOnly {
			t.Errorf("want exit %d got %d", cli.ExitTwoDirectoriesOnly, exitCode)
		}
		if err == nil {
			t.Error("expected error for wrong directory count")
		}
	})

	t.Run("NoXMLConfig", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()
		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: nil, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitNoConfig {
			t.Errorf("want exit %d got %d", cli.ExitNoConfig, exitCode)
		}
		if err == nil {
			t.Error("expected error for missing config")
		}
	})
}

func TestIntegration_EdgeCases(t *testing.T) {
	t.Run("EmptyFiles", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create empty files
		if err := os.WriteFile(filepath.Join(dirs["src"], "empty1.txt"), []byte{}, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dirs["src"], "empty2.dat"), []byte{}, 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify empty files were copied
		for _, name := range []string{"empty1.txt", "empty2.dat"} {
			dstPath := filepath.Join(dirs["dst"], name)
			info, err := os.Stat(dstPath)
			if err != nil {
				t.Errorf("empty file %s not copied: %v", name, err)
			} else if info.Size() != 0 {
				t.Errorf("file %s should be empty, got size %d", name, info.Size())
			}
		}
	})

	t.Run("LargeFile", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create a 10MB file
		largeContent := make([]byte, 10*1024*1024)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}
		largePath := filepath.Join(dirs["src"], "large.bin")
		if err := os.WriteFile(largePath, largeContent, 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify large file was copied correctly
		dstPath := filepath.Join(dirs["dst"], "large.bin")
		copiedContent, err := os.ReadFile(dstPath)
		if err != nil {
			t.Errorf("large file not copied: %v", err)
		} else if len(copiedContent) != len(largeContent) {
			t.Errorf("large file size mismatch: want %d got %d", len(largeContent), len(copiedContent))
		} else {
			// Verify a few sample bytes
			for i := 0; i < len(largeContent); i += 1024 * 1024 {
				if copiedContent[i] != largeContent[i] {
					t.Errorf("large file content mismatch at offset %d: want %d got %d", i, largeContent[i], copiedContent[i])
					break
				}
			}
		}
	})

	t.Run("SpecialCharactersInFilenames", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create files with special characters (that are valid on Windows)
		specialFiles := []string{
			"file with spaces.txt",
			"file-with-dashes.txt",
			"file_with_underscores.txt",
			"file.multiple.dots.txt",
			"file(with)parens.txt",
			"file[with]brackets.txt",
		}

		for _, name := range specialFiles {
			content := []byte("content for " + name)
			if err := os.WriteFile(filepath.Join(dirs["src"], name), content, 0o644); err != nil {
				t.Fatalf("failed to create %s: %v", name, err)
			}
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify all files were copied
		for _, name := range specialFiles {
			dstPath := filepath.Join(dirs["dst"], name)
			if _, err := os.Stat(dstPath); err != nil {
				t.Errorf("file %s not copied: %v", name, err)
			}
		}
	})

	t.Run("IdenticalContentDifferentNames", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create multiple files with identical content
		identicalContent := []byte("same content everywhere")
		for i := 1; i <= 5; i++ {
			name := fmt.Sprintf("file%d.txt", i)
			if err := os.WriteFile(filepath.Join(dirs["src"], name), identicalContent, 0o644); err != nil {
				t.Fatal(err)
			}
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify all files were copied despite identical content
		for i := 1; i <= 5; i++ {
			name := fmt.Sprintf("file%d.txt", i)
			dstPath := filepath.Join(dirs["dst"], name)
			content, err := os.ReadFile(dstPath)
			if err != nil {
				t.Errorf("file %s not copied: %v", name, err)
			} else if string(content) != string(identicalContent) {
				t.Errorf("file %s content mismatch", name)
			}
		}
	})

	t.Run("MultiSource_OverlappingFilenames", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src1", "src2", "dst")
		defer cleanup()

		// Both sources have a file with the same name but different content
		if err := os.WriteFile(filepath.Join(dirs["src1"], "shared.txt"), []byte("from source 1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dirs["src2"], "shared.txt"), []byte("from source 2"), 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src1"], dirs["src2"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src1"], dirs["src2"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify the file exists (one version should win)
		dstPath := filepath.Join(dirs["dst"], "shared.txt")
		content, err := os.ReadFile(dstPath)
		if err != nil {
			t.Errorf("shared.txt not copied: %v", err)
		} else {
			// Either source is valid
			contentStr := string(content)
			if contentStr != "from source 1" && contentStr != "from source 2" {
				t.Errorf("unexpected content in shared.txt: %q", contentStr)
			}
		}
	})

	t.Run("DeepNestedHierarchy", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create a deeply nested directory structure (10 levels deep)
		deepPath := dirs["src"]
		for i := 1; i <= 10; i++ {
			deepPath = filepath.Join(deepPath, fmt.Sprintf("level%d", i))
		}
		if err := os.MkdirAll(deepPath, 0o755); err != nil {
			t.Fatal(err)
		}

		// Put a file at the deepest level
		deepFile := filepath.Join(deepPath, "deep.txt")
		if err := os.WriteFile(deepFile, []byte("very deep content"), 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify the deep file was copied
		expectedDstPath := dirs["dst"]
		for i := 1; i <= 10; i++ {
			expectedDstPath = filepath.Join(expectedDstPath, fmt.Sprintf("level%d", i))
		}
		expectedDstPath = filepath.Join(expectedDstPath, "deep.txt")

		content, err := os.ReadFile(expectedDstPath)
		if err != nil {
			t.Errorf("deep file not copied: %v", err)
		} else if string(content) != "very deep content" {
			t.Errorf("deep file content mismatch: got %q", content)
		}
	})

	t.Run("DummyMode_NoCopy", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create source file
		if err := os.WriteFile(filepath.Join(dirs["src"], "test.txt"), []byte("should not be copied"), 0o644); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, DummyMode: true, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify file was NOT copied in dummy mode
		dstPath := filepath.Join(dirs["dst"], "test.txt")
		if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
			t.Error("file should not be copied in dummy mode")
		}

		// Verify log mentions the copy operation
		if !strings.Contains(logBuf.String(), "Copy from:") {
			t.Error("dummy mode should log copy intentions")
		}
	})

	t.Run("ReadOnlyFiles", func(t *testing.T) {
		// Test that readonly files are still backed up correctly
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create a readonly file
		roFile := filepath.Join(dirs["src"], "readonly.txt")
		if err := os.WriteFile(roFile, []byte("readonly content"), 0o444); err != nil {
			t.Fatal(err)
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify readonly file was copied
		dstFile := filepath.Join(dirs["dst"], "readonly.txt")
		content, err := os.ReadFile(dstFile)
		if err != nil {
			t.Errorf("readonly file not copied: %v", err)
		} else if string(content) != "readonly content" {
			t.Errorf("readonly file content mismatch")
		}
	})

	t.Run("ManyFiles", func(t *testing.T) {
		dirs, cleanup := makeTempDirs(t, "src", "dst")
		defer cleanup()

		// Create 100 small files
		for i := 0; i < 100; i++ {
			name := fmt.Sprintf("file%03d.txt", i)
			content := []byte(fmt.Sprintf("content for file %d", i))
			if err := os.WriteFile(filepath.Join(dirs["src"], name), content, 0o644); err != nil {
				t.Fatal(err)
			}
		}

		xc := newXMLCfgAt(t, dirs["dst"])
		setupVolumeConfigs(t, xc, dirs["src"], dirs["dst"])

		var logBuf, msgBuf bytes.Buffer
		cfg := Config{Destination: dirs["dst"], Sources: []string{dirs["src"]}, VolumeConfigProvider: xc, LogOutput: &logBuf, MessageWriter: &msgBuf}
		exitCode, err := Run(cfg)
		if exitCode != cli.ExitOk || err != nil {
			t.Fatalf("Run failed: exit=%d err=%v", exitCode, err)
		}

		// Verify all 100 files were copied
		for i := 0; i < 100; i++ {
			name := fmt.Sprintf("file%03d.txt", i)
			dstPath := filepath.Join(dirs["dst"], name)
			if _, err := os.Stat(dstPath); err != nil {
				t.Errorf("file %s not copied: %v", name, err)
			}
		}
	})
}
