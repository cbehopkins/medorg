package medorg

import (
	"errors"
	"io/fs"
	"os"
)

var ErrIncorrectFirstDirectory = errors.New("incorrect first")
var ErrFirstDirNotSeen = errors.New("not yet seen first concentrate dir")

type Concentrator struct {
	baseDir string
	de      *DirectoryEntry
}

// FIXME add test cases for all this
func NewConcentrator(dir string) *Concentrator {
	if dir == "" {
		return nil
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}

	itm := new(Concentrator)
	itm.baseDir = dir
	return itm
}
func (con *Concentrator) DirectoryVisit(de DirectoryEntry, directory string) error {
	if con.de == nil {
		// The first directory!
		if de.dir != directory {
			return ErrIncorrectFirstDirectory
		}
		if con.baseDir != directory {
			return ErrIncorrectFirstDirectory
		}
		con.de = &de
		return nil
	}
	return nil
}

// Visiter is what we need to call for each file
func (con Concentrator) Visiter(dm DirectoryMap, directory, file string, d fs.DirEntry) error {
	if con.de == nil {
		return ErrFirstDirNotSeen
	}
	err := MoveFile(NewFpath(directory, file), NewFpath(con.de.dir, file))
	if err != nil {
		return err
	}
	fileStruct, ok := dm.Get(file)
	if !ok {
		return errors.New("missing file in concentrator mover")
	}
	fileStruct.directory = con.baseDir
	con.de.dm.Add(fileStruct)
	return nil
}
