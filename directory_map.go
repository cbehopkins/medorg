package medorg

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// ErrKey - an error has been detected in the key of this struct
var ErrKey = errors.New("KV not match")
var errStructProblem = errors.New("structure Problem")

// ErrUnimplementedVisitor you have not supplied a Visitor func, and then tried to walk.
var ErrUnimplementedVisitor = errors.New("unimplemented visitor")

var errSelfCheckProblem = errors.New("self check problem")

// DirectoryMap contains for the directory all the file structs
type DirectoryMap struct {
	mp    map[string]FileStruct
	stale *bool
	// We want to copy the DirectoryMap elsewhere
	lock *sync.RWMutex

	VisitFunc func(DirectoryMap, string, string, fs.DirEntry) error
}

// NewDirectoryMap creates a new dm
func NewDirectoryMap() *DirectoryMap {
	itm := new(DirectoryMap)
	itm.mp = make(map[string]FileStruct)
	itm.stale = new(bool)
	itm.lock = new(sync.RWMutex)
	itm.VisitFunc = func(dm DirectoryMap, directory, file string, d fs.DirEntry) error {
		return ErrUnimplementedVisitor
	}
	return itm
}

// Len gth of the directoty map
func (dm DirectoryMap) Len() int {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return len(dm.mp)
}

// ToMd5File returns the dm as an md5 file
// i.e. why the hell are we not just using that?
func (dm DirectoryMap) ToMd5File(dir string) (*Md5File, error) {
	m5f := Md5File{
		Dir: dir,
	}
	dm.lock.RLock()
	defer dm.lock.RUnlock()

	for key, value := range dm.mp {
		if key == value.Name {
			m5f.append(value)
		} else {
			return nil, ErrKey
		}
	}
	return &m5f, nil
}

//ToXML
func (dm DirectoryMap) ToXML(dir string) (output []byte, err error) {
	m5f, err := dm.ToMd5File(dir)
	if err != nil {
		return nil, err
	}
	return xml.MarshalIndent(m5f, "", "  ")
}

// FromXML
func (dm *DirectoryMap) FromXML(input []byte) (dir string, err error) {
	var m5f Md5File
	err = xml.Unmarshal(input, &m5f)
	if err != nil {
		return "", err
	}
	for _, val := range m5f.Files {
		dm.Add(val)
	}
	return m5f.Dir, nil
}

// Add adds a file struct to the dm
func (dm DirectoryMap) Add(fs FileStruct) {
	dm.lock.Lock()
	fn := fs.Name
	dm.mp[fn] = fs
	*dm.stale = true
	dm.lock.Unlock()
}

// Rm Removes a filename from the dm
func (dm DirectoryMap) Rm(fn string) {
	dm.lock.Lock()
	delete(dm.mp, fn)
	*dm.stale = true
	dm.lock.Unlock()
}

// RmFile is similar to rm, but updates the directory
func (dm DirectoryMap) RmFile(dir, fn string) error {
	dm.Rm(fn)
	err := dm.Persist(dir)
	if err != nil {
		return err
	}
	return rmFilename(NewFpath(dir, fn))

}

// Get the struct associated with a filename
func (dm DirectoryMap) Get(fn string) (FileStruct, bool) {
	dm.lock.RLock()
	fs, ok := dm.mp[fn]
	dm.lock.RUnlock()
	return fs, ok
}

// DirectoryMapFromDir reads in the dirmap from the supplied dir
// It does not check anything or compute anythiing
func DirectoryMapFromDir(directory string) (dm DirectoryMap, err error) {
	// Read in the xml structure to a map/array
	dm = *NewDirectoryMap()
	if dm.mp == nil {
		return dm, errors.New("initialize malfunction")
	}
	fn := filepath.Join(directory, Md5FileName)
	var f *os.File
	_, err = os.Stat(fn)

	if errors.Is(err, os.ErrNotExist) {
		// The MD5 file not existing is not an error,
		// as long as there are no files in the directory,
		// or it is the first time we've gone into it
		return dm, nil
	}
	f, err = os.Open(fn)

	if err != nil {
		return dm, fmt.Errorf("%w error opening directory map file, %s/%s", err, directory, fn)
	}
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}
	err = f.Close()
	if err != nil {
		return
	}
	_, err = dm.FromXML(byteValue)
	if err != nil {
		return dm, fmt.Errorf("FromXML error \"%w\" on %s", err, directory)
	}

	fc := func(fn string, fs FileStruct) (FileStruct, error) {
		fs.directory = directory
		return fs, nil
	}

	return dm, dm.rangeMutate(fc)
}

// Stale returns true if the dm has been modified since writted
func (dm DirectoryMap) Stale() bool {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return *dm.stale
}

// selfCheck the directory map for obvious errors
func (dm DirectoryMap) selfCheck(directory string) error {
	fc := func(fn string, fs FileStruct) error {
		if fs.Directory() != directory {
			return fmt.Errorf("%w FS has directory of %s for %s/%s", errSelfCheckProblem, fs.Directory(), directory, fn)
		}
		return nil
	}
	return dm.rangeMap(fc)
}

func (dm DirectoryMap) pruneEmptyFile(directory, fn string, fs FileStruct, delete bool) error {
	if fs.Directory() != directory {
		return errStructProblem
	}
	if fs.Size == 0 {
		log.Println("Zero Length File")
		if delete {
			err := dm.RmFile(directory, fn)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// rangeMap do a map over the map
// Note, you may not edit the dm itself
func (dm DirectoryMap) rangeMap(fc func(string, FileStruct) error) error {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	for fn, v := range dm.mp {
		err := fc(fn, v)
		if err != nil {
			return err
		}
	}
	return nil
}

var errDeleteThisEntry = errors.New("please delete this entry - thank you kindly")
var errIgnoreThisMutate = errors.New("do not mutate this entry")

// rangeMutate range over the map, mutating as needed
// note one may return specific errors to delete or squash the mutation
func (dm DirectoryMap) rangeMutate(fc func(string, FileStruct) (FileStruct, error)) error {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	deleteList := []string{}

	for fn, v := range dm.mp {
		fs, err := fc(fn, v)
		switch err {
		case nil:
			dm.mp[fn] = fs
			*dm.stale = true
		case errIgnoreThisMutate:
		case errDeleteThisEntry:
			// Have I been writing too much python if I think this is a good idea?
			deleteList = append(deleteList, fn)
		default:
			return err
		}
	}
	if len(deleteList) > 0 {
		*dm.stale = true
		for _, v := range deleteList {
			delete(dm.mp, v)
		}
	}
	return nil
}

// RunFsFc lookup the FileStruct for the requested file
// and run the supplied function
func (dm DirectoryMap) RunFsFc(directory, file string, fc func(fs *FileStruct) error) error {

	fs, ok := dm.Get(file)
	var err error
	if !ok {
		fs, err = NewFileStruct(directory, file)
		if err != nil {
			return nil
		}
	}
	err = fc(&fs)
	if err != nil {
		return err
	}
	dm.Add(fs)

	return nil
}

// UpdateChecksum will recalc the checksum of an entry
func (dm DirectoryMap) UpdateChecksum(directory, file string, forceUpdate bool) error {
	if Debug && file == "" {
		return errors.New("asked to update a checksum on a null filename")
	}

	fc := func(fs *FileStruct) error {
		return fs.UpdateChecksum(forceUpdate)
	}
	return dm.RunFsFc(directory, file, fc)
}

// DeleteMissingFiles Delete any file entries that are in the dm,
// but not on the disk
// FIXME write a test for this
func (dm DirectoryMap) DeleteMissingFiles() error {
	fc := func(fileName string, fs FileStruct) (FileStruct, error) {
		fp := filepath.Join(fs.directory, fileName)
		_, err := os.Stat(fp)
		if errors.Is(err, os.ErrNotExist) {
			return fs, errDeleteThisEntry
		}
		return fs, errIgnoreThisMutate
	}
	return dm.rangeMutate(fc)
}

// Persist self to disk
func (dm DirectoryMap) Persist(directory string) error {
	err := dm.selfCheck(directory)
	if err != nil {
		return err
	}
	prepare := func() (bool, error) {
		dm.lock.Lock()
		defer dm.lock.Unlock()
		if !*dm.stale {
			return true, nil
		}
		*dm.stale = false
		if len(dm.mp) == 0 {
			return true, md5FileWrite(directory, nil)
		}
		return false, nil
	}

	if ret, err := prepare(); ret { // sneaky trick to save messing with defer locks
		return err
	}
	// Write out a new Xml from the structure
	ba, err := dm.ToXML(directory)
	switch err {
	case nil:
	case io.EOF:
	default:
		return fmt.Errorf("unknown Error Marshalling Xml:%w", err)
	}
	return md5FileWrite(directory, ba)
}

// Visitor satisfies DirectoryEntryInterface
// It's saying, the walker is visiting this file.
func (dm DirectoryMap) Visitor(directory, file string, d fs.DirEntry) error {
	// Note the difference between this and VisitFunc
	// We pass self in, so that the worker func can be declared once
	// rather than always having to be a closure
	// This is slightly odd, but requires fewer closures - and the costs associated
	return dm.VisitFunc(dm, directory, file, d)
}

// UpdateValues in the DirectoryEntry to those found on the fs
func (dm DirectoryMap) UpdateValues(directory string, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return err
	}
	file := d.Name()
	fs, ok := dm.Get(file)
	if changed, err := fs.Changed(info); ok && !changed {
		return err
	}
	_, err = fs.FromStat(directory, file, info)
	if err != nil {
		return err
	}
	dm.Add(fs)
	return nil
}
func (dm DirectoryMap) Copy() DirectoryEntryJournalableInterface {
	cp := NewDirectoryMap()
	for k, v := range dm.mp {
		cp.mp[k] = v
	}
	cp.VisitFunc = dm.VisitFunc
	return cp
}
func (dm0 DirectoryMap) Equal(dm DirectoryEntryInterface) bool {
	dm1 := dm.(*DirectoryMap)
	dm0.lock.RLock()
	defer dm0.lock.RUnlock()
	dm1.lock.RLock()
	defer dm1.lock.RUnlock()
	if len(dm0.mp) != len(dm1.mp) {
		return false
	}
	for k, v := range dm0.mp {
		v1, ok := dm1.mp[k]
		if !ok {
			return false
		}
		if !v.Equal(v1) {
			return false
		}
	}
	for k, v := range dm1.mp {
		v1, ok := dm0.mp[k]
		if !ok {
			return false
		}
		if !v.Equal(v1) {
			return false
		}
	}
	return true
}
