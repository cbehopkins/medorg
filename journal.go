package medorg

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Journal is a representation of our filesystem in a journaled fashion
// This is a flawed (initial) design that is very memory heavy
// A future design will remove the fl struct and instead point to the disk
// file handle and offset
type Journal struct {
	// The file list as recorded on the disk journal
	fl []DirectoryEntryJournalableInterface
	// The  location in the file list of the most recent fl entry
	location map[string]int
}

var errFileExistsInJournal = errors.New("file exists already")

func (jo Journal) String() string {
	return fmt.Sprint(jo.fl)
}

var errJournalSelfCheckFail = errors.New("journal self check fail")

// selfCheck runs a number of design rules
func (jo Journal) selfCheck() error {
	if len(jo.fl) < len(jo.location) {
		return errJournalSelfCheckFail
	}
	return nil
}

func (jo Journal) directoryExists(de DirectoryEntryJournalableInterface, dir string) bool {
	location, ok := jo.location[dir]
	if !ok {
		return false
	}
	// If they are the same, then say they are the same
	// otherwise we will behave as if this entry does not already exist
	return jo.fl[location].Equal(de)
}

func (jo *Journal) appendItem(de DirectoryEntryJournalableInterface, dir string) error {
	// log.Println("Adding Item to journal:", dir, *md5fp)
	jo.location[dir] = len(jo.fl)
	jo.fl = append(jo.fl, de.Copy()) /// FIXME NOW
	// FIXME when we implement the file handling for this
	// do the append to the file, here.
	// More likely, send it to a buffered channel.
	return nil
}

// AppendJournalFromDm adds changed dms to the journal
// It's important to note that (for now) we journal the full directory contents.
// Therefore to delete a directory in the journal, make it empty
func (jo *Journal) AppendJournalFromDm(dm DirectoryEntryJournalableInterface, dir string) error {
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
			return errFileExistsInJournal
		}
		return nil
	}

	dirExists := jo.directoryExists(dm, dir)
	err = jo.appendItem(dm, dir)
	if err != nil {
		return err
	}

	if dirExists {
		return errFileExistsInJournal
	}
	return nil
}

func (jo Journal) Range(visitor func(DirectoryEntryJournalableInterface, string) error) error {
	err := jo.selfCheck()
	if err != nil {
		return err
	}
	for dir, location := range jo.location {
		de := jo.fl[location]
		err := visitor(de, dir)
		if err != nil {
			return err
		}
	}
	return nil
}

var errShortWrite = errors.New("short write in journal")

// DumpWriter dumps the whole journal to a writer
func (jo Journal) DumpWriter(fd io.Writer) error {
	visitor := func(de DirectoryEntryJournalableInterface, dir string) error {
		xm, err := de.ToXML(dir)
		if err != nil {
			return err
		}
		n, err := fd.Write(xm)
		if err != nil {
			return err
		}
		if n != len(xm) {
			return fmt.Errorf("%w with %s", errShortWrite, de)
		}
		return nil
	}
	return jo.Range(visitor)
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

func slupReadFunc(fd io.Reader, fc func(string) error) error {
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

// SlurpReader slurps the whole file in
func (jo *Journal) SlurpReader(fd io.Reader) error {
	if jo.location == nil {
		jo.location = make(map[string]int)
	}
	fc := func(ip string) error {
		de := NewDirectoryMap()
		dir, err := de.FromXML([]byte(ip))
		if err != nil {
			return err
		}
		jo.appendItem(de, dir)
		return nil
	}
	return slupReadFunc(fd, fc)
}

var errJournalValidLen = errors.New("valid Journal Length not equal")
var errJournalMissingFile = errors.New("journal is missing file")

func (jo0 Journal) Equals(jo1 Journal, missingFunc func(DirectoryEntryJournalableInterface, string) error) error {
	if len(jo0.location) != len(jo1.location) {
		return errJournalValidLen
	}
	refJ := jo1
	fc := func(de DirectoryEntryJournalableInterface, dir string) error {
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
