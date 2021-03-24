package medorg

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// fpath is used to indicate we are talking about the full file path
type fpath string

func (f fpath) String() string {
	return string(f)
}
func Fpath(directory, fn string) fpath {
	return fpath(filepath.Join(directory, fn))
}

// removeMd5 removes the md5 file
func removeMd5(directory string) {
	fn := filepath.Join(directory, Md5FileName)
	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		_ = os.Remove(fn)
	}
}

// RmFile removes a file in a convenent fashion
// updating the xml as it goes
func RmFile(dir, fn string) error {
	dm := DirectoryMapFromDir(dir)
	return dm.RmFile(dir, fn)
}

// FileExist tests if a file exists in a convenient fashion
func FileExist(directory, fn string) bool {
	fp := filepath.Join(directory, fn)
	_, err := os.Stat(fp)
	return !os.IsNotExist(err)
}

// isDir is a quick check that it is a directory
// func isDir(directory, fn string) bool {
// 	fp := directory + "/" + fn
// 	stat, err := os.Stat(fp)
// 	if os.IsNotExist(err) {
// 		return false
// 	}
// 	if !stat.IsDir() {
// 		return false
// 	}
// 	return true
// }

// MvFile moves a dile updating the md5 files as it goes
func MvFile(srcDir, srcFn, dstDir, dstFn string) error {
	var srcDm, dstDm DirectoryMap
	srcDm = DirectoryMapFromDir(srcDir)
	if srcDir == dstDir {
		dstDm = srcDm
	} else {
		dstDm = DirectoryMapFromDir(dstDir)
	}

	err := MoveFile(Fpath(srcDir, srcFn), Fpath(dstDir, dstFn))
	if err != nil {
		return err
	}

	srcDm.Rm(srcFn)
	srcDm.WriteDirectory(srcDir)
	dstFs, err := NewFileStruct(dstDir, dstFn)
	if err != nil {
		return err
	}
	dstDm.Add(*dstFs)
	dstDm.WriteDirectory(dstDir)

	return nil
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst fpath) (err error) {
	srcs := string(src)
	dsts := string(dst)
	sfi, err := os.Stat(srcs)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dsts)
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
	if err = os.Link(srcs, dsts); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}
func rmFilename(fn fpath) error {
	fns := string(fn)
	if _, err := os.Stat(fns); err == nil {
		// FIXME return an error here
		_ = os.Remove(fns)
	}
	return nil
}

// RemoveFile simply deletes the file from disk
func RemoveFile(fn fpath) error {
	// FIXME remove this function
	rmFilename(fn)
	return nil
}

// MoveFile Implements a move function that works across file systems
// The inbuilt functions can struggle if hard links won't work
// i.e. you want to move between mount points
func MoveFile(src, dst fpath) (err error) {
	srcs := string(src)
	dsts := string(dst)
	if _, err := os.Stat(srcs); os.IsNotExist(err) {
		return err
	}
	if _, err := os.Stat(dsts); os.IsExist(err) {
		return err
	}
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
func copyFileContents(src, dst fpath) (err error) {
	srcs := string(src)
	dsts := string(dst)
	in, err := os.Open(srcs)
	if err != nil {
		return
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dsts)
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

// LoadFile load in a filename and return the data a line at a time in the channel
func LoadFile(filename string) (theChan chan string) {
	theChan = make(chan string)
	go func() {
		defer close(theChan)
		if filename == "" {
			return
		}
		f, err := os.Open(filename)
		if err == os.ErrNotExist {
			return
		} else if err != nil {
			if os.IsNotExist(err) {
				return
			}
			fmt.Printf("error opening  LoadFile file:%s: %T\n", filename, err)
			os.Exit(1)
			return
		}
		defer func() { _ = f.Close() }()

		r := bufio.NewReader(f)
		for s, e := Readln(r); e == nil; s, e = Readln(r) {
			//= strings.TrimSpace(s)
			comment := strings.HasPrefix(s, "//")
			comment = comment || strings.HasPrefix(s, "#")
			if comment {
				continue
			}
			if s == "" {
				continue
			}
			theChan <- s
		}
	}()
	return
}

// Readln reads a whole line of input
func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix = true
		err      error
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

// HomeDir returns the user's home directory
func HomeDir() fpath {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	return fpath(usr.HomeDir)
}

// AfConfig return the location of the xml config file if it exists in a known place
func AfConfig() fpath {
	fn := filepath.Join(string(HomeDir()), "/.autofix")
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = filepath.Join(string(HomeDir()), "/home/.autofix")
	}
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = ""
	}
	return fpath(fn)
}

// XmConfig return the location of the xml config file if it exists in a known place
func XmConfig() fpath {
	fn := filepath.Join(string(HomeDir()), "/.medorg.xml")
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = filepath.Join(string(HomeDir()), "/home/.medorg.xml")
	}
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = ""
	}
	return fpath(fn)
}
