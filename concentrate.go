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
	dm      *DirectoryMap
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
func (con *Concentrator) DirectoryVisit(dm DirectoryMap, directory string) error {
	if con.dm == nil {
		// The first directory!
		if con.baseDir != directory {
			return ErrIncorrectFirstDirectory
		}
		con.dm = &dm
		return nil
	}
	return nil
}

// Visiter is what we need to call for each file
func (con Concentrator) Visiter(dm DirectoryMap, directory, file string, d fs.DirEntry) error {
	if con.dm == nil {
		return ErrFirstDirNotSeen
	}
	err := MoveFile(NewFpath(directory, file), NewFpath(con.baseDir, file))
	if err != nil {
		return err
	}
	fileStruct, ok := dm.Get(file)
	if !ok {
		return errors.New("missing file in concentrator mover")
	}
	fileStruct.directory = con.baseDir
	con.dm.Add(fileStruct)
	return nil
}
