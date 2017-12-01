package medorg

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"strings"
)

// tempfilename is a useful helper function
// always gives a temp filename you can write to
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

// removeMd5 removes the md5 file
func removeMd5(directory string) {
	fn := directory + "/" + Md5FileName
	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		os.Remove(fn)
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
	fp := directory + "/" + fn
	_, err := os.Stat(fp)
	return !os.IsNotExist(err)
}

// MvFile moves a dile updating the md5 files as it goes
func MvFile(srcDir, srcFn, dstDir, dstFn string) error {
	var srcDm, dstDm DirectoryMap
	srcDm = DirectoryMapFromDir(srcDir)
	if srcDir == dstDir {
		dstDm = srcDm
	} else {
		dstDm = DirectoryMapFromDir(dstDir)
	}
	srcDm.Rm(srcFn)
	dstFs := FsFromName(dstDir, dstFn)
	dstDm.Add(dstFs)
	dstDm.WriteDirectory(dstDir)

	return MoveFile(srcDir+"/"+srcFn, dstDir+"/"+dstFn)
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
func rmFilename(fn string) {
	if _, err := os.Stat(fn); err == nil {
		os.Remove(fn)
	}
}

// RemoveFile simply deletes the file from disk
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
		defer f.Close()

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
func HomeDir() string {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	return usr.HomeDir
}

// AfConfig return the location of the xml config file if it exists in a known place
func AfConfig() string {
	fn := HomeDir() + "/.autofix"
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = HomeDir() + "/home/.autofix"
	}
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = ""
	}
	return fn
}

// XmConfig return the location of the xml config file if it exists in a known place
func XmConfig() string {
	fn := HomeDir() + "/.medorg.xml"
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = HomeDir() + "/home/.medorg.xml"
	}
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		fn = ""
	}
	return fn
}
