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

// LoadFileIter loads a file and returns its lines via a channel
// it yields non-comment, non-empty lines from a file as an iterator
// Yields tuples of (line, error) so caller can handle errors properly
func LoadFileIter(filename string) func(yield func(string, error) bool) {
	return func(yield func(string, error) bool) {
		if filename == "" {
			return
		}
		f, err := os.Open(filename)
		if err != nil {
			if !os.IsNotExist(err) {
				yield("", fmt.Errorf("error opening file %s: %w", filename, err))
			}
			return
		}
		defer func() {
			if closeErr := f.Close(); closeErr != nil {
				yield("", fmt.Errorf("error closing file %s: %w", filename, closeErr))
			}
		}()

		r := bufio.NewReader(f)
		var lastErr error
		for s, e := readln(r); e == nil; s, e = readln(r) {
			lastErr = e
			comment := strings.HasPrefix(s, "//")
			comment = comment || strings.HasPrefix(s, "#")
			if comment {
				continue
			}
			if s == "" {
				continue
			}
			if !yield(s, nil) {
				return
			}
		}
		if lastErr != nil && lastErr != io.EOF {
			yield("", fmt.Errorf("error reading file %s: %w", filename, lastErr))
		}
	}
}

// readln reads a whole line of input
// Only needed by LoadFile, which we should improve on...
func readln(r *bufio.Reader) (string, error) {
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

func ConfigPath(file string) string {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		return filepath.Join(string(HomeDir()), file)
	}
	return file
}
