package core

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"os"
	"path/filepath"
	"sync"
)

// ErrKey - an error has been detected in the key of this struct
var ErrKey = errors.New("KV not match")

// ErrUnimplementedVisitor you have not supplied a Visitor func, and then tried to walk.
var ErrUnimplementedVisitor = errors.New("unimplemented visitor")

var errSelfCheckProblem = errors.New("self check problem")

// DirectoryMap contains for the directory all the file structs
type DirectoryMap struct {
	mp    map[string]FileStruct
	stale *bool
	// We want to copy the DirectoryMap elsewhere
	lock *sync.RWMutex

	VisitFunc func(dm DirectoryMap, directory, file string, d fs.DirEntry) error
}

// NewDirectoryMap creates a new dm
func NewDirectoryMap() *DirectoryMap {
	itm := new(DirectoryMap)
	itm.mp = make(map[string]FileStruct)
	itm.stale = new(bool)
	itm.lock = new(sync.RWMutex)
	// Does not need to be protected by a lock as it is set once here
	// Or becomes the responsibility of the user to not moduify it concurrently
	itm.VisitFunc = func(dm DirectoryMap, directory, file string, d fs.DirEntry) error {
		return ErrUnimplementedVisitor
	}
	return itm
}

// SetVisitFunc sets the visitor function with proper synchronization
func (dm *DirectoryMap) SetVisitFunc(f func(dm DirectoryMap, directory, file string, d fs.DirEntry) error) {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	dm.VisitFunc = f
}

// Len gth of the directoty map
func (dm DirectoryMap) Len() int {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return len(dm.mp)
}

// ToMd5File converts the directory map to an Md5File structure
// This is primarily used for XML serialization
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

// ToXML marshals the directory map to XML format
func (dm DirectoryMap) ToXML(dir string) (output []byte, err error) {
	m5f, err := dm.ToMd5File(dir)
	if err != nil {
		return nil, err
	}
	return xml.MarshalIndent(m5f, "", "  ")
}

// ToXMLWithAlias is deprecated - alias is no longer part of Md5File
// Use ToXML instead
func (dm DirectoryMap) ToXMLWithAlias(dir, alias string) (output []byte, err error) {
	return dm.ToXML(dir)
}

// FromXML unmarshals XML data into the directory map and returns the directory path
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

// rm Removes a filename from the dm
func (dm DirectoryMap) rm(fn string) {
	dm.lock.Lock()
	delete(dm.mp, fn)
	*dm.stale = true
	dm.lock.Unlock()
}

// RmFile is similar to rm, but updates the directory
func (dm DirectoryMap) RmFile(dir, fn string) error {
	dm.rm(fn)
	err := dm.Persist(dir)
	if err != nil {
		return err
	}
	return RmFilename(NewFpath(dir, fn))
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
	mdFilePath := filepath.Join(directory, Md5FileName)
	var f *os.File
	_, err = os.Stat(mdFilePath)

	if errors.Is(err, os.ErrNotExist) {
		// The MD5 file not existing is not an error,
		// as long as there are no files in the directory,
		// or it is the first time we've gone into it
		return dm, nil
	}
	f, err = os.Open(mdFilePath)
	if err != nil {
		return dm, fmt.Errorf("%w error opening directory map file, %s/%s", err, directory, mdFilePath)
	}
	byteValue, err := io.ReadAll(f)
	if err != nil {
		return
	}
	err = f.Close()
	if err != nil {
		return
	}
	_, err = dm.FromXML(byteValue)
	err = supressXmlUnmarshallErrors(err)
	if err != nil {
		return dm, fmt.Errorf("FromXML error \"%w\" on %s", err, directory)
	}

	fc := func(fn string, fs FileStruct) (FileStruct, error) {
		fs.directory = directory
		return fs, nil
	}

	return dm, dm.RangeMutate(fc)
}

// Stale returns true if the dm has been modified since writted
func (dm DirectoryMap) Stale() bool {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return *dm.stale
}

// Interface-based access methods for better decoupling

// GetMetadata retrieves file metadata by filename
// Returns the metadata and whether it was found
func (dm DirectoryMap) GetMetadata(fn string) (FileMetadata, bool) {
	fs, ok := dm.Get(fn)
	if !ok {
		return nil, false
	}
	return &fs, true
}

// AddMetadata adds file metadata to the directory map
func (dm DirectoryMap) AddMetadata(fm FileMetadata) {
	// Convert FileMetadata to FileStruct if it's already one
	if fs, ok := fm.(*FileStruct); ok {
		dm.Add(*fs)
		return
	}
	// Otherwise create a new FileStruct from the metadata
	// Extract the fields we need (Size, Checksum, Tags are fields not methods)
	fs := FileStruct{
		directory:  fm.Directory(),
		BackupDest: fm.BackupDestinations(),
	}
	// We can only add FileStruct instances, so this is a limitation
	// for now - the metadata must be a FileStruct pointer
	dm.Add(fs)
}

// DirectoryStorage interface implementation

// Load reads metadata from storage (implements DirectoryStorage.Load)
func (dm *DirectoryMap) Load(directory string) error {
	panic("I think this should be unused")
	loadedDm, err := DirectoryMapFromDir(directory)
	if err != nil {
		return err
	}
	*dm = loadedDm
	return nil
}

// Save writes metadata to storage (implements DirectoryStorage.Save)
func (dm DirectoryMap) Save(directory string) error {
	panic("I think this should be unused")
	return dm.Persist(directory)
}

// GetFile retrieves metadata for a specific file (implements DirectoryStorage.GetFile)
func (dm DirectoryMap) GetFile(filename string) (FileMetadata, error) {
	panic("I think this should be unused")
	fm, ok := dm.GetMetadata(filename)
	if !ok {
		return nil, fmt.Errorf("file %s not found in directory map", filename)
	}
	return fm, nil
}

// AddFile adds or updates file metadata (implements DirectoryStorage.AddFile)
func (dm DirectoryMap) AddFile(fm FileMetadata) error {
	dm.AddMetadata(fm)
	return nil
}

// RemoveFile removes file metadata (implements DirectoryStorage.RemoveFile)
func (dm DirectoryMap) RemoveFile(filename string) error {
	dir, fn := filepath.Split(filename)
	dm.RmFile(dir, fn)
	return nil
}

// ListFiles returns all files in the directory (implements DirectoryStorage.ListFiles)
func (dm DirectoryMap) ListFiles() []FileMetadata {
	result := make([]FileMetadata, 0, dm.Len())
	err := dm.ForEachFile(func(filename string, fm FileMetadata) error {
		result = append(result, fm)
		return nil
	})
	if err != nil {
		// ForEachFile shouldn't fail with our simple append function
		return result
	}
	return result
}

// ForEachFile iterates over all files in the directory map
// The callback receives the filename and file metadata
// This is a read only operation
func (dm DirectoryMap) ForEachFile(fn func(string, FileMetadata) error) error {
	dm.lock.RLock()
	defer dm.lock.RUnlock()

	for name, fs := range dm.mp {
		fsCopy := fs // Create a copy to avoid pointer issues
		if err := fn(name, &fsCopy); err != nil {
			return err
		}
	}
	return nil
}

// selfCheck the directory map for obvious errors
func (dm DirectoryMap) selfCheck(directory string) error {
	fc := func(fn string, fs FileMetadata) error {
		if fs.Directory() != directory {
			return fmt.Errorf("%w FS has directory of %s for %s/%s", errSelfCheckProblem, fs.Directory(), directory, fn)
		}
		return nil
	}
	return dm.ForEachFile(fc)
}

var (
	// ErrDeleteThisEntry sentinel error to request deletion of an entry during mutation
	ErrDeleteThisEntry = errors.New("please delete this entry - thank you kindly")
	// ErrIgnoreThisMutate sentinel error to skip mutation of an entry
	ErrIgnoreThisMutate = errors.New("do not mutate this entry")
)

// RangeMutate range over the map, mutating as needed
// note one may return specific errors to delete or squash the mutation
func (dm DirectoryMap) RangeMutate(fc func(string, FileStruct) (FileStruct, error)) error {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	deleteList := []string{}

	for fn, v := range dm.mp {
		fs, err := fc(fn, v)
		switch err {
		case nil:
			dm.mp[fn] = fs
			*dm.stale = true
		case ErrIgnoreThisMutate:
		case ErrDeleteThisEntry:
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
// This is intended as a test helper function
func (dm DirectoryMap) UpdateChecksum(directory, file string, forceUpdate bool) error {
	if Debug && file == "" {
		return errors.New("asked to update a checksum on a null filename")
	}
	log.Println("Updating checksum for", directory, file)
	fc := func(fs *FileStruct) error {
		return fs.UpdateChecksum(forceUpdate)
	}
	return dm.RunFsFc(directory, file, fc)
}

// DeleteMissingFiles deletes any file entries that are in the dm,
// but not on the disk
func (dm DirectoryMap) DeleteMissingFiles() error {
	fc := func(fileName string, fs FileStruct) (FileStruct, error) {
		fp := filepath.Join(fs.directory, fileName)
		_, err := os.Stat(fp)
		if errors.Is(err, os.ErrNotExist) {
			return fs, ErrDeleteThisEntry
		}
		return fs, ErrIgnoreThisMutate
	}
	return dm.RangeMutate(fc)
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
	dm.lock.RLock()
	visitFunc := dm.VisitFunc
	dm.lock.RUnlock()
	return visitFunc(dm, directory, file, d)
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
	dm.lock.RLock()
	maps.Copy(cp.mp, dm.mp)
	visitFunc := dm.VisitFunc
	dm.lock.RUnlock()
	cp.lock.Lock()
	cp.VisitFunc = visitFunc
	cp.lock.Unlock()
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

func (dm DirectoryMap) Revisit(dir string, visitor func(dm DirectoryEntryInterface, directory string, file string, fileStruct FileStruct) error) error {
	for path, fileStruct := range dm.mp {
		if err := visitor(dm, dir, path, fileStruct); err != nil {
			return fmt.Errorf("visitor error for file %s in directory %s: %w", path, dir, err)
		}
	}
	if err := dm.Persist(dir); err != nil {
		return fmt.Errorf("error persisting directory after revisit of %s: %w", dir, err)
	}
	return nil
}
