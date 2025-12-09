package consumers

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	core "github.com/cbehopkins/medorg/pkg/core"
)

// JournalEntry wraps a DirectoryMap with alias information for journal storage
type JournalEntry struct {
	dm    core.DirectoryEntryJournalableInterface
	dir   string
	alias string
}

// ToXML serializes the journal entry with alias information
func (je JournalEntry) ToXML() ([]byte, error) {
	if je.alias != "" {
		// If we have an alias, use ToXMLWithAlias
		if dm, ok := je.dm.(*core.DirectoryMap); ok {
			return dm.ToXMLWithAlias(je.dir, je.alias)
		}
	}
	// Fallback to regular ToXML
	return je.dm.ToXML(je.dir)
}

// Journal is a representation of our filesystem in a journaled fashion
// Entries are appended to a buffered channel and processed asynchronously
// This design is more memory efficient than the original approach which kept
// everything in memory
type Journal struct {
	// The file list as recorded on the disk journal
	fl []JournalEntry
	// The location in the file list of the most recent fl entry
	location map[string]int
	// Channel for buffering entries to be written asynchronously
	entryChan chan JournalEntry
	// WaitGroup to track pending write operations
	writeWg sync.WaitGroup
	// Channel to signal that the writer goroutine should stop
	stopChan chan struct{}
	// Mutex to protect concurrent access to fl and location
	mu sync.RWMutex
	// Atomic flag to track if journal is closed (1 = closed, 0 = open)
	closed atomic.Uint32
}

var (
	ErrFileExistsInJournal  = errors.New("file exists already")
	errJournalSelfCheckFail = errors.New("journal self check fail")
	errJournalValidLen      = errors.New("valid Journal Length not equal")
	errJournalMissingFile   = errors.New("journal is missing file")
)

// NewJournal creates a new Journal with a buffered channel for asynchronous entry processing
// bufferSize controls the size of the entry buffer (default recommended: 100)
func NewJournal(bufferSize int) *Journal {
	jo := &Journal{
		fl:        make([]JournalEntry, 0),
		location:  make(map[string]int),
		entryChan: make(chan JournalEntry, bufferSize),
		stopChan:  make(chan struct{}),
	}
	// Start the background writer goroutine
	jo.writeWg.Add(1)
	go jo.entryWriter()
	return jo
}

// entryWriter runs in a background goroutine and processes entries from the channel
func (jo *Journal) entryWriter() {
	defer jo.writeWg.Done()
	for {
		select {
		case entry, ok := <-jo.entryChan:
			if !ok {
				// Channel closed, exit
				return
			}
			// Process the entry
			jo.mu.Lock()
			jo.location[entry.dir] = len(jo.fl)
			jo.fl = append(jo.fl, entry)
			jo.mu.Unlock()
		case <-jo.stopChan:
			// Stop signal received, drain remaining entries before exiting
			close(jo.entryChan)
			for entry := range jo.entryChan {
				jo.mu.Lock()
				jo.location[entry.dir] = len(jo.fl)
				jo.fl = append(jo.fl, entry)
				jo.mu.Unlock()
			}
			return
		}
	}
}

// Flush waits for all pending entries to be written and stops the background writer
// Safe to call multiple times - subsequent calls are no-ops
// Safe to call on zero-value Journal (no-op)
func (jo *Journal) Flush() {
	// If stopChan is nil, this is a zero-value Journal with no async behavior
	if jo.stopChan == nil {
		return
	}

	// Mark as closed atomically
	if !jo.closed.CompareAndSwap(0, 1) {
		// Already closed by another goroutine, just wait
		jo.writeWg.Wait()
		return
	}

	// We successfully transitioned from open to closed, close the channel
	close(jo.stopChan)
	jo.writeWg.Wait()
}

func (jo *Journal) String() string {
	jo.mu.RLock()
	defer jo.mu.RUnlock()
	return fmt.Sprint(jo.fl)
}

// selfCheck runs a number of design rules
func (jo *Journal) selfCheck() error {
	jo.mu.RLock()
	defer jo.mu.RUnlock()
	if len(jo.fl) < len(jo.location) {
		return errJournalSelfCheckFail
	}
	return nil
}

func (jo *Journal) directoryExists(de core.DirectoryEntryJournalableInterface, dir string) bool {
	jo.mu.RLock()
	location, ok := jo.location[dir]
	if !ok {
		jo.mu.RUnlock()
		return false
	}
	// Get the entry while holding the lock
	entry := jo.fl[location]
	jo.mu.RUnlock()
	// If they are the same, then say they are the same
	// otherwise we will behave as if this entry does not already exist
	return entry.dm.Equal(de)
}

func (jo *Journal) appendItem(de core.DirectoryEntryJournalableInterface, dir, alias string) error {
	entry := JournalEntry{
		dm:    de.Copy(),
		dir:   dir,
		alias: alias,
	}

	// If entryChan is nil (zero-value initialization), write directly
	// Otherwise use async channel for performance
	if jo.entryChan == nil {
		// Synchronous path - direct write
		jo.mu.Lock()
		jo.location[entry.dir] = len(jo.fl)
		jo.fl = append(jo.fl, entry)
		jo.mu.Unlock()
	} else {
		// Asynchronous path - check if closed before sending
		if jo.closed.Load() != 0 {
			return errors.New("journal is closed")
		}
		// Send to background writer (still might fail if closed concurrently, but that's ok)
		select {
		case jo.entryChan <- entry:
			// Successfully sent
		case <-jo.stopChan:
			// Journal is shutting down, return error
			return errors.New("journal is closed")
		}
	}
	return nil
}

// AppendJournalFromDm adds changed dms to the journal
// It's important to note that (for now) we journal the full directory contents.
// Therefore to delete a directory in the journal, make it empty
// FIXME This should not be needed???
func (jo *Journal) AppendJournalFromDm(dm core.DirectoryEntryJournalableInterface, dir string) error {
	err := jo.selfCheck()
	if err != nil {
		return err
	}
	if jo.location == nil {
		jo.location = make(map[string]int)
	}

	// Should we delete the entry?
	if dm.Len() == 0 {
		_, ok := jo.location[dir]
		if ok {
			delete(jo.location, dir)
			return ErrFileExistsInJournal
		}
		return nil
	}

	dirExists := jo.directoryExists(dm, dir)
	err = jo.appendItem(dm, dir, "")
	if err != nil {
		return err
	}

	if dirExists {
		return ErrFileExistsInJournal
	}
	return nil
}

// AppendJournalFromDmWithAlias adds changed dms to the journal with alias information
// The alias is stored in the journal for restore operations
func (jo *Journal) AppendJournalFromDmWithAlias(dm core.DirectoryEntryJournalableInterface, dir, alias string) error {
	// FIXME I don't think we should self check every single append!
	err := jo.selfCheck()
	if err != nil {
		return err
	}
	if jo.location == nil {
		jo.location = make(map[string]int)
	}

	// Should we delete the entry?
	if dm.Len() == 0 {
		_, ok := jo.location[dir]
		if ok {
			delete(jo.location, dir)
			return ErrFileExistsInJournal
		}
		return nil
	}

	dirExists := jo.directoryExists(dm, dir)
	err = jo.appendItem(dm, dir, alias)
	if err != nil {
		return err
	}

	if dirExists {
		return ErrFileExistsInJournal
	}
	return nil
}

// Range over the valid items in the journal
func (jo *Journal) Range(visitor func(core.DirectoryEntryJournalableInterface, string) error) error {
	err := jo.selfCheck()
	if err != nil {
		return err
	}
	jo.mu.RLock()
	locationCopy := make(map[string]int)
	for k, v := range jo.location {
		locationCopy[k] = v
	}
	flCopy := make([]JournalEntry, len(jo.fl))
	copy(flCopy, jo.fl)
	jo.mu.RUnlock()

	for dir, location := range locationCopy {
		entry := flCopy[location]
		err := visitor(entry.dm, dir)
		if err != nil {
			return err
		}
	}
	return nil
}

var errShortWrite = errors.New("short write in journal")

// ToWriter dumps the whole journal to a writer
func (jo *Journal) ToWriter(fd io.Writer) error {
	err := jo.selfCheck()
	if err != nil {
		return err
	}

	jo.mu.RLock()
	locationCopy := make(map[string]int)
	for k, v := range jo.location {
		locationCopy[k] = v
	}
	flCopy := make([]JournalEntry, len(jo.fl))
	copy(flCopy, jo.fl)
	jo.mu.RUnlock()

	// Write each entry directly using the JournalEntry's ToXML
	for _, location := range locationCopy {
		entry := flCopy[location]
		xm, err := entry.ToXML()
		if err != nil {
			return err
		}
		n, err := fd.Write(xm)
		if err != nil {
			return err
		}
		if n != len(xm) {
			return fmt.Errorf("%w with %s", errShortWrite, entry.dir)
		}
	}
	return nil
}

// scanToken returns a token which for us is an xml token
// i.e. token wil contain:
// (any text up to the match)<anXmlToken>
// the key here is a potential token starts with an open brace and ends at the next close brace
func scanToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
	openFound := false
	searchToken := byte('<')
	for i := 0; i < len(data); i++ {
		if data[i] == searchToken {
			if openFound {
				return i + 1, data[:i+1], nil
			}
			openFound = true
			searchToken = byte('>')
		}
	}
	if !atEOF {
		return 0, nil, nil
	}
	// There is one final token to be delivered, which may be the empty string.
	// Returning bufio.ErrFinalToken here tells Scan there are no more tokens after this
	// but does not trigger an error to be returned from Scan itself.
	return 0, data, bufio.ErrFinalToken
}

func getRecord(scanner *bufio.Scanner) (string, error) {
	// myTxt should get a record at a time, i.e. <dr>....</dr>
	var b strings.Builder
	for scanner.Scan() {
		txt := scanner.Text() // don't want to allocate twice
		b.WriteString(txt)
		// search the minimum text possible
		if strings.HasSuffix(txt, "</dr>") {
			return b.String(), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return b.String(), err
	}
	myTxt := strings.TrimSpace(b.String())
	if myTxt == "" {
		return myTxt, io.EOF
	}
	return myTxt, errors.New("failed to find an end token in scanner")
}

func SlupReadFunc(fd io.Reader, fc func(string) error) error {
	scanner := bufio.NewScanner(fd)
	scanner.Split(scanToken)
	for record, err := getRecord(scanner); ; record, err = getRecord(scanner) {
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		err = fc(record)
		if err != nil {
			return err
		}
	}
}

// FromReader slurps the whole file in
func (jo *Journal) FromReader(fd io.Reader) error {
	if jo.location == nil {
		jo.location = make(map[string]int)
	}
	fc := func(ip string) error {
		de := core.NewDirectoryMap()
		dir, err := de.FromXML([]byte(ip))
		if err != nil {
			return err
		}
		// When reading from file, we don't have alias info in old format
		// In new format, the alias is embedded in the XML
		return jo.appendItem(de, dir, "")
	}
	return SlupReadFunc(fd, fc)
}

func (jo0 *Journal) Equals(jo1 *Journal, missingFunc func(core.DirectoryEntryJournalableInterface, string) error) error {
	jo0.mu.RLock()
	len0 := len(jo0.location)
	jo0.mu.RUnlock()

	jo1.mu.RLock()
	len1 := len(jo1.location)
	jo1.mu.RUnlock()

	if len0 != len1 {
		return errJournalValidLen
	}
	refJ := jo1
	fc := func(de core.DirectoryEntryJournalableInterface, dir string) error {
		if !refJ.directoryExists(de, dir) {
			if missingFunc == nil {
				return fmt.Errorf("%w,%s", errJournalMissingFile, de)
			}
			return missingFunc(de, dir)
		}
		return nil
	}
	err := jo0.Range(fc)
	if err != nil {
		return err
	}
	refJ = jo0
	return jo1.Range(fc)
}
