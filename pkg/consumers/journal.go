package consumers

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"

	core "github.com/cbehopkins/medorg/pkg/core"
)

// JournalEntry is simply an Md5File - a directory with its files
type JournalEntry = core.Md5File

// Journal is a representation of our filesystem in a journaled fashion
// Maps each alias to a list of JournalEntry entries (directories with their files)
type Journal struct {
	// Map from alias to list of directory entries
	entries map[string][]JournalEntry
	// Mutex to protect concurrent access
	mu sync.RWMutex
}

var (
	ErrFileExistsInJournal = errors.New("file exists already")
	ErrAliasRequired       = errors.New("alias required")
)

// NewJournal creates a new Journal
func NewJournal() *Journal {
	return &Journal{
		entries: make(map[string][]JournalEntry),
	}
}

func (jo *Journal) String() string {
	jo.mu.RLock()
	defer jo.mu.RUnlock()
	return fmt.Sprint(jo.entries)
}

// Append adds a directory's files to the journal under the specified alias
// This is called during directory traversal - once per directory
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
		// Convert FileMetadata back to FileStruct
		if fs, ok := fm.(*core.FileStruct); ok {
			files = append(files, *fs)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Create JournalEntry (which is just an Md5File)
	entry := JournalEntry{
		Dir:   dir,
		Files: files,
	}

	// Add to journal under this alias
	jo.mu.Lock()
	jo.entries[alias] = append(jo.entries[alias], entry)
	jo.mu.Unlock()

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
func (jo *Journal) ToWriter(w io.Writer) error {
	jo.mu.RLock()
	defer jo.mu.RUnlock()

	for alias, entries := range jo.entries {
		// Write opening tag with alias
		fmt.Fprintf(w, "<mdj alias=\"%s\">\n", alias)

		// Write each directory entry
		for _, entry := range entries {
			entryXML, err := xml.MarshalIndent(entry, "  ", "  ")
			if err != nil {
				return err
			}
			w.Write([]byte("  "))
			w.Write(entryXML)
			w.Write([]byte("\n"))
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

				// Add to journal
				jo.mu.Lock()
				jo.entries[alias] = append(jo.entries[alias], entry)
				jo.mu.Unlock()
			}
		}
	}

	return nil
}
