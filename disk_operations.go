package medorg

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

func tempfilename(dirName string, create bool) string {
	if dirName == "" {
		dirName = os.TempDir()
	}
	tmpfile, err := ioutil.TempFile(dirName, "grabTemp_")
	if err != nil {
		log.Fatal("Unable to create a temporary file!", err)
	}
	filename := tmpfile.Name()
	tmpfile.Close()
	if !create {
		os.Remove(filename)
	}
	return filename
}
func rmFilename(fn string) {
	if _, err := os.Stat(fn); err == nil {
		os.Remove(fn)
	}
}
func FileExist(directory, fn string) bool {
	fp := directory + "/" + fn
	_, err := os.Stat(fp)
	return !os.IsNotExist(err)
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}
func RemoveFile(fn string) error {
	rmFilename(fn)
	return nil
}

// MoveFile Implements a move function that works across file systems
// The inbuilt functions can struccle if hard links won't work
// i.e. you want to move between mount points
func MoveFile(src, dst string) (err error) {
	err = CopyFile(src, dst)
	if err != nil {
		log.Fatalf("Copy problem\nType:%T\nVal:%v\n", err, err)
	}
	return RemoveFile(src)
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}
