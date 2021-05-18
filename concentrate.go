package medorg

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// ErrIncorrectFirstDirectory is raised if the firct directory visited
// is not the base directory you provided under creation
var ErrIncorrectFirstDirectory = errors.New("incorrect first")

// ErrFirstDirNotSeen is returned when we visit a file before the first directory is visited
var ErrFirstDirNotSeen = errors.New("not yet seen first concentrate dir")

type Concentrator struct {
	BaseDir string
	dm      *DirectoryMap
}

func (con *Concentrator) initDir() error {
	if con.BaseDir == "" {
		con.BaseDir = "."
	}
	if _, err := os.Stat(con.BaseDir); os.IsNotExist(err) {
		return fmt.Errorf("%w::Not a valid concentration directory", err)
	}
	return nil
}

// DirectoryVisit is something to call on every directory you are interested in
func (con *Concentrator) DirectoryVisit(dm DirectoryMap, directory string) error {
	err := con.initDir()
	if err != nil {
		return err
	}
	if con.dm == nil {
		// The first directory!
		if con.BaseDir != directory {
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
	err := MoveFile(NewFpath(directory, file), NewFpath(con.BaseDir, file))
	if err != nil {
		return err
	}
	fileStruct, ok := dm.Get(file)
	if !ok {
		return errors.New("missing file in concentrator mover")
	}
	fileStruct.directory = con.BaseDir
	con.dm.Add(fileStruct)
	return nil
}
