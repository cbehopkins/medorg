package medorg

import (
	"errors"
	"fmt"
	"time"
)

// Journal is a representation of our filesystem in a journaled fashion
// This is a flawed (initial) design that is very memory heavy
// A future design will remove the fl struct and instead point to the disk
// file handle and offset
type Journal struct {
	// The file list as recorded on the disk journal
	fl []Md5File
	// The  location in the file list of the most recent fl entry
	location map[string]int
}

var errFileExistsInJournal = errors.New("file exists already")

func (jo Journal) String() string {
	return fmt.Sprint(jo.fl)
}

// Len returns the number of directories in the struct
// Note a directory can have >1 entries in the file list
// but only 1 entry in the location list
func (jo Journal) Len() int {
	return len(jo.location)
}

// selfCheck runs a number of design rules
func (jo Journal) selfCheck() bool {
	if len(jo.fl) < len(jo.location) {
		return false
	}

	return true
}

func (jo Journal) directoryExists(md5fp *Md5File, dir string) bool {
	location, ok := jo.location[dir]
	if !ok {
		return false
	}
	// If they are the same, then say they are the same
	// otherwise we will behave as if this entry does not already exist
	return jo.fl[location].Equal(*md5fp)
}

func (jo *Journal) appendItem(md5fp *Md5File, dir string) error {
	// log.Println("Adding Item to journal:", dir, *md5fp)
	jo.location[dir] = len(jo.fl)
	jo.fl = append(jo.fl, *md5fp)
	// FIXME when we implement the file handling for this
	// do the append to the file, here.
	// More likely, send it to a buffered channel.
	return nil
}

// AppendJournalFromDm adds changed dms to the journal
// It's important to note that (for now) we journal the full directory contents.
// Therefore to delete a directory in the journal, make it empty
func (jo *Journal) AppendJournalFromDm(dm DirectoryEntryInterface, dir string) error {
	if jo.location == nil {
		jo.location = make(map[string]int)
	}
	md5fp, err := dm.ToMd5File()

	if err != nil {
		return err
	}
	md5fp.Dir = dir
	md5fp.Ts = time.Now().Unix()
	dirExists := jo.directoryExists(md5fp, dir)
	if len(md5fp.Files) == 0 {
		delete(jo.location, dir)
		if dirExists {
			return errFileExistsInJournal
		}
		return nil
	}
	err = jo.appendItem(md5fp, dir)
	if err != nil {
		return err
	}

	if dirExists {
		return errFileExistsInJournal
	}
	return nil
}
