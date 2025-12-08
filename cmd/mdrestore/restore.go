package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cbehopkins/medorg/pkg/cli"
	"github.com/cbehopkins/medorg/pkg/core"
)

// Config holds the configuration for the restore operation
type Config struct {
	JournalPath string
	SourceDir   string
	XMLConfig   *core.MdConfig
	Stdout      io.Writer
}

// Run executes the restore operation
func Run(cfg Config) (int, error) {
	// Step 2: Read volume label from source directory
	sourceVolumeLabel, err := cfg.XMLConfig.GetVolumeLabel(cfg.SourceDir)
	if err != nil {
		return cli.ExitRestoreError, fmt.Errorf("failed to read volume label from source: %w", err)
	}
	fmt.Fprintf(cfg.Stdout, "Source volume label: %s\n", sourceVolumeLabel)

	// Step 3: Read journal to understand expected files
	journal, err := readJournal(cfg.JournalPath)
	if err != nil {
		return cli.ExitRestoreError, fmt.Errorf("failed to read journal: %w", err)
	}
	fmt.Fprintf(cfg.Stdout, "Read journal with %d entries\n", len(journal.Entries))

	// Step 4: Map journal aliases to restore destinations from XMLCfg
	aliasToDestination := make(map[string]string)
	for _, entry := range journal.Entries {
		if entry.Alias == "" {
			continue
		}
		if _, ok := aliasToDestination[entry.Alias]; !ok {
			// Get restore destination from config
			destPath, found := cfg.XMLConfig.GetRestoreDestination(entry.Alias)
			if !found {
				// If no restore destination configured, use source path
				srcDir, found := cfg.XMLConfig.GetSourceDirectory(entry.Alias)
				if !found {
					fmt.Fprintf(cfg.Stdout, "Warning: no restore destination or source for alias '%s', skipping\n", entry.Alias)
					continue
				}
				destPath = srcDir.Path
			}
			aliasToDestination[entry.Alias] = destPath
			fmt.Fprintf(cfg.Stdout, "Alias '%s' -> '%s'\n", entry.Alias, destPath)
		}
	}

	// Step 5: Run check_calc in each destination directory
	destChecksums := make(map[string]*core.DirectoryMap)
	for alias, destPath := range aliasToDestination {
		if _, err := os.Stat(destPath); os.IsNotExist(err) {
			fmt.Fprintf(cfg.Stdout, "Warning: destination '%s' for alias '%s' does not exist, skipping\n", destPath, alias)
			continue
		}
		fmt.Fprintf(cfg.Stdout, "Calculating checksums for %s...\n", destPath)
		dm, err := calculateChecksums(destPath)
		if err != nil {
			fmt.Fprintf(cfg.Stdout, "Warning: failed to calculate checksums for %s: %v\n", destPath, err)
			continue
		}
		destChecksums[alias] = dm
	}

	// Step 6-9: Process each journal entry
	restored := 0
	skipped := 0
	missingVolumes := make(map[string]bool)

	for _, entry := range journal.Entries {
		if entry.Alias == "" {
			continue
		}

		destPath, ok := aliasToDestination[entry.Alias]
		if !ok {
			continue
		}

		destDM, ok := destChecksums[entry.Alias]
		if !ok {
			continue
		}

		for _, file := range entry.Files {
			// Check if file exists in destination with correct MD5
			destFile, exists := destDM.Get(file.Name)
			needsCopy := !exists || destFile.Checksum != file.Hash

			if needsCopy {
				// Check if this file's volume matches current source volume
				if file.BackupDest == sourceVolumeLabel {
					// Step 7: Copy from source
					srcFilePath := filepath.Join(cfg.SourceDir, entry.Dir, file.Name)
					destFilePath := filepath.Join(destPath, entry.Dir, file.Name)

					// Ensure parent directories exist before copy
					if err := os.MkdirAll(filepath.Dir(destFilePath), 0o755); err != nil {
						fmt.Fprintf(cfg.Stdout, "Error preparing destination for %s: %v\n", file.Name, err)
						continue
					}

					if err := copyFile(srcFilePath, destFilePath); err != nil {
						fmt.Fprintf(cfg.Stdout, "Error copying %s: %v\n", file.Name, err)
						continue
					}
					restored++
					fmt.Fprintf(cfg.Stdout, "Restored: %s\n", file.Name)
				} else {
					// Step 8: Note which volume has the file
					missingVolumes[file.BackupDest] = true
				}
			} else {
				skipped++
			}
		}
	}

	// Step 10: Print summary
	fmt.Fprintf(cfg.Stdout, "\nRestore Summary:\n")
	fmt.Fprintf(cfg.Stdout, "  Files restored: %d\n", restored)
	fmt.Fprintf(cfg.Stdout, "  Files skipped (already correct): %d\n", skipped)

	if len(missingVolumes) > 0 {
		fmt.Fprintf(cfg.Stdout, "\nMissing volumes needed to complete restore:\n")
		for vol := range missingVolumes {
			fmt.Fprintf(cfg.Stdout, "  - %s\n", vol)
		}
		fmt.Fprintf(cfg.Stdout, "\nRun mdrestore again with each missing volume to complete the restore.\n")
	} else {
		fmt.Fprintf(cfg.Stdout, "\nRestore complete!\n")
	}

	return cli.ExitOk, nil
}

// JournalEntry represents an entry in the journal
type JournalEntry struct {
	Alias string
	Dir   string
	Files []JournalFile
}

// JournalFile represents a file in a journal entry
type JournalFile struct {
	Name       string
	Hash       string
	BackupDest string
}

// Journal represents the entire journal
type Journal struct {
	Entries []JournalEntry
}

// readJournal reads and parses the journal file
func readJournal(path string) (*Journal, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read the entire file
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	journal := &Journal{}

	// Parse XML records directly to preserve alias
	content := string(data)
	for len(content) > 0 {
		// Find next <dr> tag
		start := findNext(content, "<dr")
		if start == -1 {
			break
		}
		// Find the end of this record
		end := findNext(content[start:], "</dr>")
		if end == -1 {
			break
		}
		end += start + 5 // Add start offset and length of "</dr>"

		// Extract the XML record
		record := content[start:end]

		// Parse this record
		var m5f core.Md5File
		if err := xml.Unmarshal([]byte(record), &m5f); err != nil {
			return nil, fmt.Errorf("failed to parse journal record: %w", err)
		}

		// Create journal entry
		entry := JournalEntry{
			Dir:   m5f.Dir,
			Alias: m5f.Alias,
		}

		// Extract file information
		for _, fs := range m5f.Files {
			for _, bd := range fs.BackupDest {
				entry.Files = append(entry.Files, JournalFile{
					Name:       fs.Name,
					Hash:       fs.Checksum,
					BackupDest: bd,
				})
			}
		}

		journal.Entries = append(journal.Entries, entry)

		// Move to next record
		content = content[end:]
	}

	return journal, nil
}

// findNext finds the next occurrence of a substring
func findNext(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// calculateChecksums runs check_calc on a directory
func calculateChecksums(dir string) (*core.DirectoryMap, error) {
	dm, err := core.DirectoryMapFromDir(dir)
	if err != nil {
		return nil, err
	}

	// Walk the directory and compute checksums for all files
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() == core.Md5FileName {
			return nil
		}

		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		// Check if we already have this file in the map
		if _, exists := dm.Get(relPath); !exists {
			fs, err := core.NewFileStruct(dir, relPath)
			if err != nil {
				return err
			}
			dm.Add(fs)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &dm, nil
}

// copyFileFunc is a variable that points to the actual copy implementation.
// This allows tests to inject custom implementations.
var copyFileFunc = copyFileImpl

// copyFile copies a file from src to dst using the injected function
func copyFile(src, dst string) error {
	return copyFileFunc(src, dst)
}

// copyFileImpl is the actual implementation of file copying
func copyFileImpl(src, dst string) error {
	// Ensure destination directory exists
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Copy to temporary file first for atomic replace
	tmp := dst + ".tmp"
	// Best effort cleanup of any stale temp file
	_ = os.Remove(tmp)

	if err := core.CopyFile(core.Fpath(src), core.Fpath(tmp)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to copy to temp file: %w", err)
	}

	// Verify checksum before final rename
	sdir, sbase := filepath.Split(src)
	tdir, tbase := filepath.Split(tmp)
	sh, err := core.CalcMd5File(sdir, sbase)
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to checksum src: %w", err)
	}
	th, err := core.CalcMd5File(tdir, tbase)
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to checksum temp: %w", err)
	}
	if sh != th {
		_ = os.Remove(tmp)
		return fmt.Errorf("checksum mismatch after copy")
	}

	// Remove existing destination if present to avoid rename issues on Windows
	if _, err := os.Stat(dst); err == nil {
		_ = os.Remove(dst)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to finalize copy: %w", err)
	}
	return nil
}
