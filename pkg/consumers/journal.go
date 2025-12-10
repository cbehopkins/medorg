package consumers

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	core "github.com/cbehopkins/medorg/pkg/core"
)

// JournalEntry is simply an Md5File - a directory with its files
type JournalEntry = core.Md5File

// TmpJournalFile represents a temporary file that stores XML-serialized JournalEntry objects
// All entries for a given alias are appended to this file
type TmpJournalFile struct {
	// Path to the temporary file on disk
	filePath string
	// File handle for appending entries
	file *os.File
	// Mutex to protect concurrent access to the file
	mu sync.Mutex
}

// Journal is a representation of our filesystem in a journaled fashion
// Maps each alias to a TmpJournalFile that stores the entries on disk
type Journal struct {
	// Map from alias to temporary journal file
	entries map[string]*TmpJournalFile
	// Base directory for temporary files
	tmpDir string
	// Mutex to protect concurrent access to the map
	mu sync.RWMutex
	// List of Regexs to ignore files
	ignoreList []regexp.Regexp
}

var (
	ErrFileExistsInJournal = errors.New("file exists already")
	ErrAliasRequired       = errors.New("alias required")
)

// NewJournal creates a new Journal with a temporary directory for storing entry files
func NewJournal() (*Journal, error) {
	tmpDir, err := os.MkdirTemp("", "medorg-journal-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %w", err)
	}
	return &Journal{
		entries: make(map[string]*TmpJournalFile),
		tmpDir:  tmpDir,
	}, nil
}

func (jo *Journal) AddIgnorePattern(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("failed to compile ignore pattern %s: %w", pattern, err)
	}
	jo.mu.Lock()
	defer jo.mu.Unlock()
	jo.ignoreList = append(jo.ignoreList, *re)
	return nil
}

// getOrCreateTmpFile returns the TmpJournalFile for the given alias, creating it if necessary
func (jo *Journal) getOrCreateTmpFile(alias string) (*TmpJournalFile, error) {
	jo.mu.Lock()
	defer jo.mu.Unlock()

	if tmpFile, exists := jo.entries[alias]; exists {
		return tmpFile, nil
	}

	// Create a new temporary file for this alias
	filePath := filepath.Join(jo.tmpDir, fmt.Sprintf("journal-%s.xml", alias))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file for alias %s: %w", alias, err)
	}

	tmpFile := &TmpJournalFile{
		filePath: filePath,
		file:     file,
	}
	jo.entries[alias] = tmpFile
	return tmpFile, nil
}

func (jo *Journal) String() string {
	jo.mu.RLock()
	defer jo.mu.RUnlock()

	var result strings.Builder
	for alias := range jo.entries {
		result.WriteString(fmt.Sprintf("alias: %s\n", alias))
	}
	return result.String()
}

// Close closes all temporary files and cleans up resources
func (jo *Journal) Close() error {
	jo.mu.Lock()
	defer jo.mu.Unlock()

	for alias, tmpFile := range jo.entries {
		if tmpFile.file != nil {
			if err := tmpFile.file.Close(); err != nil {
				return fmt.Errorf("failed to close temporary file for alias %s: %w", alias, err)
			}
		}
	}
	return nil
}

// Cleanup removes all temporary files associated with this journal
func (jo *Journal) Cleanup() error {
	if err := jo.Close(); err != nil {
		return err
	}
	return os.RemoveAll(jo.tmpDir)
}

func (jo *Journal) shouldIgnore(path string) bool {
	jo.mu.RLock()
	defer jo.mu.RUnlock()
	for _, re := range jo.ignoreList {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

// Append adds a directory's files to the journal under the specified alias
// This is called during directory traversal - once per directory
// The entry is written to a temporary file on disk, not kept in memory
func (jo *Journal) Append(dm core.DirectoryMap, dir, alias string) error {
	if dm.Len() == 0 {
		return nil // Skip empty directories
	}
	if strings.TrimSpace(alias) == "" {
		return ErrAliasRequired
	}

	// Extract files from DirectoryMap
	var files []core.FileStruct
	err := dm.ForEachFile(func(filename string, fm core.FileMetadata) error {
		// Evaluate ignore patterns against the full path so directory names are respected
		fullPath := filepath.Join(dir, filename)
		if jo.shouldIgnore(fullPath) {
			return nil
		}
		// Convert FileMetadata back to FileStruct
		// FIXME: Bad type assertion - need a better way
		if fs, ok := fm.(*core.FileStruct); ok {
			files = append(files, *fs)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Skip writing empty entries (all files ignored)
	if len(files) == 0 {
		return nil
	}

	// Create JournalEntry (which is just an Md5File)
	entry := JournalEntry{
		Dir:   dir,
		Files: files,
	}

	// Get or create the temporary file for this alias
	tmpFile, err := jo.getOrCreateTmpFile(alias)
	if err != nil {
		return err
	}

	// Marshal entry to XML
	entryXML, err := xml.MarshalIndent(entry, "  ", "  ")
	if err != nil {
		return err
	}

	// Write to the temporary file
	tmpFile.mu.Lock()
	defer tmpFile.mu.Unlock()

	_, err = tmpFile.file.Write(entryXML)
	if err != nil {
		return fmt.Errorf("failed to write entry to temporary file: %w", err)
	}
	_, err = tmpFile.file.WriteString("\n")
	if err != nil {
		return fmt.Errorf("failed to write newline to temporary file: %w", err)
	}

	return nil
}

// PopulateFromDirectories walks the given directory recursively and populates the journal
// All subdirectories are included under the specified alias
func (jo *Journal) PopulateFromDirectories(directory string, alias string) error {
	if strings.TrimSpace(alias) == "" {
		return ErrAliasRequired
	}

	// Create a maker function that will be called for each directory
	// This captures the DirectoryMap and adds it to the journal
	makerFunc := func(dir string) (core.DirectoryTrackerInterface, error) {
		mkFk := func(dir string) (core.DirectoryEntryInterface, error) {
			dm, err := core.DirectoryMapFromDir(dir)
			if err != nil {
				return &dm, err
			}

			// Set a no-op visitor function
			dm.SetVisitFunc(func(dm core.DirectoryMap, directory, file string, d fs.DirEntry) error {
				return nil
			})

			// Add this directory's map to the journal
			appendErr := jo.Append(dm, dir, alias)
			if appendErr != nil {
				// Log but don't fail - we still want to continue walking
				fmt.Fprintf(io.Discard, "Warning: failed to append directory %s to journal: %v\n", dir, appendErr)
			}

			return &dm, nil
		}
		return core.NewDirectoryEntry(dir, mkFk)
	}

	dt := core.NewDirTracker(false, directory, makerFunc)
	// Wait for completion and collect errors
	for err := range dt.ErrChan() {
		if err != nil {
			return err
		}
	}

	return nil
}

// ToWriter writes the journal to XML format:
// <mdj alias="alias1">
//
//	<dr dir="dir1">...</dr>
//	<dr dir="dir2">...</dr>
//
// </mdj>
// <mdj alias="alias2">
//
//	<dr dir="dir3">...</dr>
//
// </mdj>
// Entries are read from temporary files on disk to avoid memory issues with large datasets
func (jo *Journal) ToWriter(w io.Writer) error {
	jo.mu.RLock()
	aliases := make([]string, 0, len(jo.entries))
	tmpFiles := make(map[string]*TmpJournalFile)
	for alias, tmpFile := range jo.entries {
		aliases = append(aliases, alias)
		tmpFiles[alias] = tmpFile
	}
	jo.mu.RUnlock()

	for _, alias := range aliases {
		tmpFile := tmpFiles[alias]

		// Write opening tag with alias
		fmt.Fprintf(w, "<mdj alias=\"%s\">\n", alias)

		// Flush and sync the temporary file to ensure data is written to disk
		// Only sync if file is still open (not closed by caller)
		tmpFile.mu.Lock()
		if tmpFile.file != nil {
			// Try to sync, but don't fail if file is already closed
			_ = tmpFile.file.Sync()
		}
		tmpFile.mu.Unlock()

		// Read entries from the temporary file
		file, err := os.Open(tmpFile.filePath)
		if err != nil {
			return fmt.Errorf("failed to open temporary file for alias %s: %w", alias, err)
		}
		defer file.Close()

		// Read and write each XML entry from the file
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) > 0 {
				w.Write([]byte("  "))
				w.Write(line)
				w.Write([]byte("\n"))
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading temporary file for alias %s: %w", alias, err)
		}

		// Write closing tag
		fmt.Fprintf(w, "</mdj>\n")
	}

	return nil
}

// FromReader reads journal XML from a reader in the format:
// <mdj alias="alias1">
//
//	<dr dir="...">...</dr>
//
// </mdj>
// <mdj alias="alias2">
//
//	<dr dir="...">...</dr>
//
// </mdj>
// Entries are written to temporary files on disk instead of being kept in memory
func (jo *Journal) FromReader(r io.Reader) error {
	decoder := xml.NewDecoder(r)

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Look for <mdj> opening tags
		startElem, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		if startElem.Name.Local != "mdj" {
			continue
		}

		// Extract alias from <mdj alias="...">
		var alias string
		for _, attr := range startElem.Attr {
			if attr.Name.Local == "alias" {
				alias = attr.Value
				break
			}
		}

		if strings.TrimSpace(alias) == "" {
			return ErrAliasRequired
		}

		// Get or create the temporary file for this alias
		tmpFile, err := jo.getOrCreateTmpFile(alias)
		if err != nil {
			return err
		}

		// Now decode nested <dr> entries until we hit </mdj>
		for {
			token, err := decoder.Token()
			if err == io.EOF {
				return fmt.Errorf("unexpected EOF while reading mdj element")
			}
			if err != nil {
				return err
			}

			// Check for </mdj> closing tag
			endElem, ok := token.(xml.EndElement)
			if ok && endElem.Name.Local == "mdj" {
				break
			}

			// Look for <dr> elements
			startElem, ok := token.(xml.StartElement)
			if !ok {
				continue
			}

			if startElem.Name.Local == "dr" {
				// Decode the directory entry
				var entry JournalEntry
				if err := decoder.DecodeElement(&entry, &startElem); err != nil {
					return fmt.Errorf("error decoding directory entry: %w", err)
				}

				// Marshal entry to XML and write to temporary file
				entryXML, err := xml.MarshalIndent(entry, "  ", "  ")
				if err != nil {
					return fmt.Errorf("error marshaling entry: %w", err)
				}

				tmpFile.mu.Lock()
				_, err = tmpFile.file.Write(entryXML)
				if err != nil {
					tmpFile.mu.Unlock()
					return fmt.Errorf("failed to write entry to temporary file: %w", err)
				}
				_, err = tmpFile.file.WriteString("\n")
				if err != nil {
					tmpFile.mu.Unlock()
					return fmt.Errorf("failed to write newline to temporary file: %w", err)
				}
				tmpFile.mu.Unlock()
			}
		}
	}

	return nil
}
