/*
Package main implements mddiscover - a tool to identify source files that already exist on backup destinations.

Overview:
mddiscover discovers which files in configured source directories already exist on a backup
destination by comparing MD5 checksums. When matches are found, source metadata is updated
to reference the backup destination's VolumeLabel, eliminating the need to back up files
that are already safely stored.

Operation:

Phase 1: Source Directory Analysis
  - Parse all configured source directories
  - Build a mapping from MD5 checksum to FileStruct for each discovered file
  - Validate checksum uniqueness: if two files share the same MD5 but differ in content,
    raise an error and terminate (this indicates a critical hash collision)

Phase 2: Backup Destination Analysis
  - Accept backup destination path via command line argument
  - Read the VolumeLabel from the destination (error if not present - advises running mdlabel)
  - Run checksum calculation on the destination to ensure all checksums are current
  - Build a second mapping from MD5 checksum to FileStruct for the backup destination

Phase 3: Discovery and Metadata Update
  - Compare source and destination mappings by MD5 checksum and file size
  - For each source file that exists on the destination:
  - Update the source FileStruct's BackupDest field with the destination's VolumeLabel
  - Write the updated metadata back to the source .medorg.xml file
  - This marks files as "already backed up," preventing redundant backup operations

This tool is similar to mdbackup but focuses on discovery and metadata reconciliation
rather than performing actual file copies.
*/
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/consumers"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Config holds the configuration for mddiscover
type Config struct {
	SourceDirs     []string
	DestinationDir string
	ConfigPath     string
	Stdout         io.Writer
	XMLConfig      *core.MdConfig
	DryRun         bool
}

func main() {
	var configPath string
	var dryRun bool

	flag.StringVar(&configPath, "config", "", "Path to config file (optional)")
	flag.BoolVar(&dryRun, "dry-run", false, "Show what would be discovered without updating metadata")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: mddiscover [options] <backup-destination>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Discover source files that already exist on a backup destination")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Examples:")
		fmt.Fprintln(os.Stderr, "  mddiscover E:/")
		fmt.Fprintln(os.Stderr, "  mddiscover --dry-run /mnt/backup1")
		fmt.Fprintln(os.Stderr, "  mddiscover --config custom.xml F:/Backups")
		os.Exit(cli.ExitInvalidArgs)
	}

	destinationDir := flag.Arg(0)

	// Load config using common loader
	loader := cli.NewConfigLoader(configPath, os.Stderr)
	xc, exitCode := loader.Load()
	if exitCode != cli.ExitOk {
		os.Exit(exitCode)
	}

	// Get source directories from config
	sourceDirs := xc.GetSourcePaths()
	if len(sourceDirs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: No source directories configured")
		fmt.Fprintln(os.Stderr, "Use 'mdsource add' to configure source directories")
		os.Exit(cli.ExitNoSources)
	}

	cfg := Config{
		SourceDirs:     sourceDirs,
		DestinationDir: destinationDir,
		ConfigPath:     configPath,
		Stdout:         os.Stdout,
		XMLConfig:      xc,
		DryRun:         dryRun,
	}

	cli.ExitFromRun(Run(cfg))
}

// Run executes the discovery operation
func Run(cfg Config) (int, error) {
	fmt.Fprintln(cfg.Stdout, "=== mddiscover: File Discovery ===")
	fmt.Fprintf(cfg.Stdout, "Destination: %s\n", cfg.DestinationDir)
	if cfg.DryRun {
		fmt.Fprintln(cfg.Stdout, "Mode: DRY RUN (no metadata will be updated)")
	}
	fmt.Fprintln(cfg.Stdout, "")

	// Phase 1: Build source file mapping
	fmt.Fprintln(cfg.Stdout, "Phase 1: Analyzing source directories...")
	sourceMapping, err := buildSourceMapping(cfg)
	if err != nil {
		return cli.ExitChecksumError, fmt.Errorf("source analysis failed: %w", err)
	}
	fmt.Fprintf(cfg.Stdout, "  Found %d unique files in source directories\n", len(sourceMapping))
	fmt.Fprintln(cfg.Stdout, "")

	// Phase 2: Analyze backup destination
	fmt.Fprintln(cfg.Stdout, "Phase 2: Analyzing backup destination...")

	// Check if volume label exists (GetVolumeLabel auto-creates if missing)
	labelFile := filepath.Join(cfg.DestinationDir, ".mdbackup.xml")
	if _, err := os.Stat(labelFile); os.IsNotExist(err) {
		return cli.ExitNoVolumeLabel, fmt.Errorf("no volume label found at %s\nRun 'mdlabel create %s' to create a volume label", cfg.DestinationDir, cfg.DestinationDir)
	}

	// Get volume label
	volumeLabel, err := cfg.XMLConfig.GetVolumeLabel(cfg.DestinationDir)
	if err != nil {
		return cli.ExitNoVolumeLabel, fmt.Errorf("failed to read volume label: %w", err)
	}
	fmt.Fprintf(cfg.Stdout, "  Volume Label: %s\n", volumeLabel)

	// Run checksum calculation on destination
	fmt.Fprintln(cfg.Stdout, "  Calculating checksums...")
	if err := consumers.RunCheckCalc([]string{cfg.DestinationDir}, consumers.CheckCalcOptions{
		CalcCount: 4,
		Recalc:    false,
	}); err != nil {
		return cli.ExitChecksumError, fmt.Errorf("checksum calculation failed: %w", err)
	}

	// Build destination mapping
	destMapping, err := buildDestinationMapping(cfg.DestinationDir)
	if err != nil {
		return cli.ExitChecksumError, fmt.Errorf("destination analysis failed: %w", err)
	}
	fmt.Fprintf(cfg.Stdout, "  Found %d files on destination\n", len(destMapping))
	fmt.Fprintln(cfg.Stdout, "")

	// Phase 3: Discovery and metadata update
	fmt.Fprintln(cfg.Stdout, "Phase 3: Discovering matches...")
	matchCount, updateCount, err := discoverAndUpdate(cfg, sourceMapping, destMapping, volumeLabel)
	if err != nil {
		return cli.ExitDiscoveryError, fmt.Errorf("discovery failed: %w", err)
	}

	fmt.Fprintln(cfg.Stdout, "")
	fmt.Fprintln(cfg.Stdout, "=== Summary ===")
	fmt.Fprintf(cfg.Stdout, "Files matched: %d\n", matchCount)
	if cfg.DryRun {
		fmt.Fprintf(cfg.Stdout, "Would update metadata for %d files\n", updateCount)
		fmt.Fprintln(cfg.Stdout, "(Run without --dry-run to apply changes)")
	} else {
		fmt.Fprintf(cfg.Stdout, "Metadata updated: %d files\n", updateCount)
	}

	return cli.ExitOk, nil
}

// FileKey uniquely identifies a file by checksum and size
type FileKey struct {
	Checksum string
	Size     int64
}

// SourceFileInfo tracks source file location and metadata
type SourceFileInfo struct {
	Directory  string
	FileName   string
	FileStruct core.FileStruct
}

// buildSourceMapping creates a map from (checksum, size) to source file info
func buildSourceMapping(cfg Config) (map[FileKey][]SourceFileInfo, error) {
	mapping := make(map[FileKey][]SourceFileInfo)

	for _, srcDir := range cfg.SourceDirs {
		fmt.Fprintf(cfg.Stdout, "  Scanning: %s\n", srcDir)

		// Ensure checksums are calculated
		if err := consumers.RunCheckCalc([]string{srcDir}, consumers.CheckCalcOptions{
			CalcCount: 4,
			Recalc:    false,
		}); err != nil {
			return nil, fmt.Errorf("checksum calculation failed for %s: %w", srcDir, err)
		}

		// Walk directory tree
		err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories and .medorg.xml
			if info.IsDir() || info.Name() == core.Md5FileName {
				return nil
			}

			// Get directory containing this file
			fileDir := filepath.Dir(path)

			// Load the directory map to get file metadata
			dm, err := core.DirectoryMapFromDir(fileDir)
			if err != nil {
				return fmt.Errorf("failed to load metadata for %s: %w", fileDir, err)
			}

			// Get the file struct
			fileName := filepath.Base(path)
			fs, ok := dm.Get(fileName)
			if !ok || fs.Checksum == "" {
				// File not in metadata or no checksum - skip it
				return nil
			}

			key := FileKey{
				Checksum: fs.Checksum,
				Size:     fs.Size,
			}

			// Check for collisions (same checksum/size but different files)
			if existing, exists := mapping[key]; exists {
				// Verify it's actually the same file
				for _, existingFile := range existing {
					if existingFile.FileName != fileName {
						// Different filename with same checksum/size - potential collision
						fmt.Fprintf(cfg.Stdout, "  Warning: Potential hash collision detected:\n")
						fmt.Fprintf(cfg.Stdout, "    File 1: %s (in %s)\n", existingFile.FileName, existingFile.Directory)
						fmt.Fprintf(cfg.Stdout, "    File 2: %s (in %s)\n", fileName, fileDir)
						fmt.Fprintf(cfg.Stdout, "    Checksum: %s, Size: %d\n", key.Checksum, key.Size)
					}
				}
			}

			mapping[key] = append(mapping[key], SourceFileInfo{
				Directory:  fileDir,
				FileName:   fileName,
				FileStruct: fs,
			})

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to walk %s: %w", srcDir, err)
		}
	}

	return mapping, nil
}

// buildDestinationMapping creates a map from (checksum, size) to destination file existence
func buildDestinationMapping(destDir string) (map[FileKey]bool, error) {
	mapping := make(map[FileKey]bool)

	err := filepath.Walk(destDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and .medorg.xml
		if info.IsDir() || info.Name() == core.Md5FileName || info.Name() == ".mdbackup.xml" {
			return nil
		}

		// Get directory containing this file
		fileDir := filepath.Dir(path)

		// Load the directory map
		dm, err := core.DirectoryMapFromDir(fileDir)
		if err != nil {
			return nil // Skip if no metadata
		}

		// Get the file struct
		fileName := filepath.Base(path)
		fs, ok := dm.Get(fileName)
		if !ok || fs.Checksum == "" {
			return nil // Skip if no checksum
		}

		key := FileKey{
			Checksum: fs.Checksum,
			Size:     fs.Size,
		}

		mapping[key] = true
		return nil
	})
	if err != nil {
		return nil, err
	}

	return mapping, nil
}

// discoverAndUpdate finds matches and updates source metadata
func discoverAndUpdate(cfg Config, sourceMapping map[FileKey][]SourceFileInfo, destMapping map[FileKey]bool, volumeLabel string) (int, int, error) {
	matchCount := 0
	updateCount := 0

	// Track which directory maps need to be persisted
	dirtyDirs := make(map[string]*core.DirectoryMap)

	for key, sourceFiles := range sourceMapping {
		// Check if this file exists on destination
		if !destMapping[key] {
			continue // Not on destination
		}

		matchCount++

		// Update each source file that matches
		for _, srcFile := range sourceFiles {
			// Check if already tagged with this volume label
			if srcFile.FileStruct.HasTag(volumeLabel) {
				continue // Already tagged
			}

			updateCount++
			fmt.Fprintf(cfg.Stdout, "  Match: %s/%s -> %s\n", srcFile.Directory, srcFile.FileName, volumeLabel)

			if !cfg.DryRun {
				// Load or get the directory map for this directory
				dm, exists := dirtyDirs[srcFile.Directory]
				if !exists {
					loadedDM, err := core.DirectoryMapFromDir(srcFile.Directory)
					if err != nil {
						return matchCount, updateCount, fmt.Errorf("failed to load directory map for %s: %w", srcFile.Directory, err)
					}
					dm = &loadedDM
					dirtyDirs[srcFile.Directory] = dm
				}

				// Update the file struct with the volume label
				fs, ok := dm.Get(srcFile.FileName)
				if !ok {
					continue // File disappeared?
				}

				fs.AddTag(volumeLabel)
				dm.Add(fs)
			}
		}
	}

	// Persist all dirty directory maps
	if !cfg.DryRun {
		for dir, dm := range dirtyDirs {
			if err := dm.Persist(dir); err != nil {
				return matchCount, updateCount, fmt.Errorf("failed to persist metadata for %s: %w", dir, err)
			}
		}
	}

	return matchCount, updateCount, nil
}
