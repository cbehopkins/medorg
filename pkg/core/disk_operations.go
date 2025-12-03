package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// md5WriteTokenChan limits concurrent MD5 file writes to prevent resource contention
// TODO: Consider packing files into a single zip archive in the future
var md5WriteTokenChan = MakeTokenChan(4)

// md5FileWrite write to the directory's file
// deletes the file, if the ba to write is empty
func md5FileWrite(directory string, ba []byte) error {
	<-md5WriteTokenChan
	defer func() { md5WriteTokenChan <- struct{}{} }()

	fn := filepath.Join(directory, Md5FileName)
	if _, err := os.Stat(fn); !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(fn)
	}
	if len(ba) == 0 {
		return nil
	}
	return os.WriteFile(fn, ba, 0o600)
}

// FileExist tests if a file exists in a convenient fashion
func FileExist(directory, fn string) bool {
	fp := filepath.Join(directory, fn)
	_, err := os.Stat(fp)
	return !os.IsNotExist(err)
}

func createDestDirectoryAsNeeded(dst string) error {
	dir := filepath.Dir(dst)
	stat, err := os.Stat(dir)
	if err == nil && stat.IsDir() {
		return nil
	}
	return os.MkdirAll(dir, 0o777)
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst Fpath) (err error) {
	srcs := string(src)
	dsts := string(dst)
	sfi, err := os.Stat(srcs)
	if err != nil {
		return fmt.Errorf("error in CopyFile src file status %w %s", err, srcs)
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dsts)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return nil
		}
	}
	err = createDestDirectoryAsNeeded(dsts)
	if err != nil {
		return fmt.Errorf("issue in CopyFile creating directory tree %w", err)
	}
	if err = os.Link(srcs, dsts); err == nil {
		return nil
	}
	return copyFileContents(srcs, dsts)
}

// RmFilename removes a file if it exists
func RmFilename(fn Fpath) error {
	fns := string(fn)
	if _, err := os.Stat(fns); err == nil {
		return os.Remove(fns)
	}
	return nil
}

// MoveFile Implements a move function that works across file systems
// The inbuilt functions can struggle if hard links won't work
// i.e. you want to move between mount points
func MoveFile(src, dst Fpath) (err error) {
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
		return fmt.Errorf("copy problem when moving %w", err)
	}
	return RmFilename(src)
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(srcs, dsts string) (err error) {
	in, err := os.Open(srcs)
	if err != nil {
		return fmt.Errorf("info error on src in copyFileContents : %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dsts)
	if err != nil {
		return fmt.Errorf("unable to write to output file in copyFileContents %w %s", err, dsts)
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

// LoadFile loads a file and returns its lines via a channel
// TODO: Refactor autofix initialization to avoid this utility
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
// Only needed by LoadFile, which we should improve on...
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
// Panics if unable to determine home directory (critical system error)
func HomeDir() Fpath {
	usr, err := user.Current()
	if err != nil {
		// This is a critical system error that should never happen in normal operation
		// Using panic here as this is called during initialization and there's no recovery
		panic(fmt.Sprintf("unable to get user home directory: %v", err))
	}
	return Fpath(usr.HomeDir)
}

// AfConfig return the location of the xml config file if it exists in a known place
func AfConfig() Fpath {
	fn := ConfigPath(".autofix")
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = ""
	}
	return Fpath(fn)
}

// XmConfig return the location of the xml config file if it exists in a known place
func XmConfig() Fpath {
	fn := ConfigPath(".medorg.xml")
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = ""
	}
	return Fpath(fn)
}

func ConfigPath(file string) string {
	fn := filepath.Join(string(HomeDir()), "home", file)
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = filepath.Join(string(HomeDir()), file)
	}
	return fn
}
