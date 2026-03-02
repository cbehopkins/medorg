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
	"runtime"
	"slices"
	"sync"
)

// ErrKey - an error has been detected in the key of this struct
var ErrKey = errors.New("KV not match")

// ErrUnimplementedVisitor you have not supplied a Visitor func, and then tried to walk.
var ErrUnimplementedVisitor = errors.New("unimplemented visitor")

var errSelfCheckProblem = errors.New("self check problem")

// Mutating Callback
type DmMutCallback func(file Fpath, d os.FileInfo, fs FileStruct) (FileStruct, error)

// Read only Callback
type DmVisitFuncType func(dm DirectoryMap, path Fpath, d fs.DirEntry) error
type ForEachCallback func(Fname, FileMetadata, os.FileInfo) error

// A File Struct Func allows you to run a function on a FileStruct
// You can mutate the FileStruct as needed and it will be stored back in the DirectoryMap
type FsFunc func(fs *FileStruct) error

// DirectoryMapInterface any directory object needs to support this
type DirectoryMapInterface interface {
	Persist(Dirname) error
	// Visitor(path Fpath, d fs.DirEntry) error
}

// DirectoryMapJournalableInterface if you want to store all info
// to a journal, support this
// type DirectoryMapJournalableInterface interface {
// 	DirectoryMapInterface
// 	ToXML(dir Dirname) (output []byte, err error)
// 	FromXML(input []byte) (dir Dirname, err error)
// 	Equal(DirectoryMapInterface) bool
// 	Len() int
// 	Copy() DirectoryMapJournalableInterface
// }

// DirectoryMap contains for the directory all the file structs
type DirectoryMap struct {
	mp    map[Fname]FileStruct
	fi    map[Fname]os.FileInfo
	stale *bool
	// We want to copy the DirectoryMap elsewhere
	lock *sync.RWMutex
	dir  Dirname

	visitFunc  DmVisitFuncType
	workerPool *workerPool
	// Optional shared worker pool for RangeMutate operations
	pool *mutatePool
}

type mutateWorkItem struct {
	fn       Fname
	fpath    Fpath
	fi       os.FileInfo
	fs       FileStruct
	callback DmMutCallback
	resultCh chan mutateResult
}

type mutateResult struct {
	fn  Fname
	fs  FileStruct
	err error
}

type mutatePool struct {
	workCh chan mutateWorkItem
	wg     sync.WaitGroup
}

func newMutatePool() *mutatePool {
	workerCount := max(runtime.NumCPU(), 1)
	pool := &mutatePool{
		workCh: make(chan mutateWorkItem, workerCount*2),
	}
	pool.wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer pool.wg.Done()
			for item := range pool.workCh {
				fs, err := item.callback(item.fpath, item.fi, item.fs)
				item.resultCh <- mutateResult{fn: item.fn, fs: fs, err: err}
			}
		}()
	}
	return pool
}

func (p *mutatePool) close() {
	close(p.workCh)
	p.wg.Wait()
}

func updateDirEntry(dm DirectoryMap, path Fpath, d fs.DirEntry) error {
	if path.Is(Md5FileName) {
		return nil
	}
	err := dm.UpdateValues(path.Dir(), d)
	if err != nil {
		return err
	}

	return nil
}

// newDirectoryMap creates a new dm
func newDirectoryMap() *DirectoryMap {
	itm := new(DirectoryMap)
	itm.mp = make(map[Fname]FileStruct)
	itm.fi = make(map[Fname]os.FileInfo)
	itm.stale = new(bool)
	itm.lock = new(sync.RWMutex)
	itm.visitFunc = updateDirEntry
	return itm
}

// SetVisitFunc sets the visitor function with proper synchronization
// The visit func is called during directory traversal for each file
// It runs early in the process and is quite integral to the DirectoryMap construction
// One typically only overrides this for test purposes
func (dm *DirectoryMap) SetVisitFunc(f DmVisitFuncType) {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	dm.visitFunc = f
}

// Len gth of the directory map
func (dm DirectoryMap) Len() int {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return len(dm.mp)
}

// ToMd5File converts the directory map to an Md5File structure
// This is primarily used for XML serialization
// Note: Dir field is intentionally not set for .medorg.xml files (it's superfluous)
// Journal files set Dir explicitly when needed
func (dm DirectoryMap) ToMd5File(dir Dirname) (*Md5File, error) {
	m5f := Md5File{}
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
func (dm DirectoryMap) ToXML(dir Dirname) (output []byte, err error) {
	m5f, err := dm.ToMd5File(dir)
	if err != nil {
		return nil, err
	}
	return xml.MarshalIndent(m5f, "", "  ")
}

// FromXML unmarshals XML data into the directory map and returns the directory path
func (dm *DirectoryMap) FromXML(input []byte) (dir Dirname, err error) {
	var m5f Md5File
	err = xml.Unmarshal(input, &m5f)
	if err != nil {
		return "", err
	}
	for _, val := range m5f.Files {
		dm.Add(val)
	}
	return Dirname(m5f.Dir), nil
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
func (dm DirectoryMap) rm(fn Fname) {
	dm.lock.Lock()
	delete(dm.mp, fn)
	delete(dm.fi, fn)
	*dm.stale = true
	dm.lock.Unlock()
}

// RmFile is similar to rm, but updates the directory
func (dm DirectoryMap) RmFile(dir Dirname, fn Fname) error {
	dm.rm(fn)
	err := dm.Persist(dir)
	if err != nil {
		return err
	}
	// FIXME
	return RmFilename(NewFpath(string(dir), string(fn)))
}

// Get the struct associated with a filename
func (dm DirectoryMap) Get(fn Fname) (FileStruct, bool) {
	dm.lock.RLock()
	fs, ok := dm.mp[fn]
	dm.lock.RUnlock()
	return fs, ok
}

// DirectoryMapFromDir reads in the dirmap from the supplied dir
// It does not check anything or compute anythiing
// Literally just loads the file from disk (or creates an empty one)
func DirectoryMapFromDir(directory Dirname, workerPool *workerPool) (dm DirectoryMap, err error) {
	// Read in the xml structure to a map/array
	dm = *newDirectoryMap()
	dm.dir = directory
	dm.workerPool = workerPool

	if dm.mp == nil {
		return dm, errors.New("initialize malfunction")
	}
	mdFilePath := filepath.Join(string(directory), Md5FileName)
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

	fc := func(file Fpath, d os.FileInfo, fs FileStruct) (FileStruct, error) {
		fs.directory = directory
		return fs, nil
	}

	return dm, dm.RangeMutate(fc)
}

// DirectoryMapFromDirEntries Create A Directory Map from a directory and a set of os.DirEntry
// As one can build from a single read of the directory
func DirectoryMapFromDirEntries(directory Dirname, entries []os.DirEntry, workerPool *workerPool) (DirectoryMap, error) {
	dm, err := DirectoryMapFromDir(directory, workerPool)
	if err != nil {
		return dm, err
	}

	dm.lock.Lock()
	defer dm.lock.Unlock()
	err = dm.updateFromDirEntry(directory, entries)
	if err != nil {
		return dm, err
	}
	// Any entries that have a dm, but no corresponding file info, must be stale
	for fname := range dm.mp {
		if _, ok := dm.fi[fname]; !ok {
			delete(dm.mp, fname)
			delete(dm.fi, fname)
			*dm.stale = true
		}
	}
	return dm, nil
}

// updateFromDirEntry updates the DirectoryMap from the provided os.DirEntry slice
// This critically should allow us to do the directory read once
// and update everything from that single read
// significantly reducing our IO
func (dm *DirectoryMap) updateFromDirEntry(directory Dirname, entries []os.DirEntry) error {
	for _, entry := range entries {
		fname := Fname(entry.Name())
		fi, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				dm.rm(fname)
				continue
			}
			return err
		}

		if entry.IsDir() || IsMetadataFile(entry.Name()) {
			continue
		}
		dm.fi[fname] = fi
		// Use FromStat directly with the FileInfo we already have instead of NewFileStruct
		var fs FileStruct
		fs, _ = dm.mp[fname]
		if changed, err := fs.Changed(fi); !changed && err == nil {
			continue
		}
		fs, err = fs.FromStat(directory, fname, fi)
		if err != nil {
			continue
		}
		dm.mp[fname] = fs
		*dm.stale = true
	}
	return nil
}

func (dm DirectoryMap) ChecksumCalc(workTokens chan struct{}) error {
	return dm.checksumCalc(workTokens, false)
}
func (dm DirectoryMap) checksumCalc(workTokens chan struct{}, ignoreErrors bool) error {
	if workTokens != nil {
		// It's assumed that the caller has already acquired a token
		// So we release it here
		workTokens <- struct{}{}
		defer func() { <-workTokens }()
	}

	// Phase 1: Identify files needing checksum calculation (read-only)
	var needsChecksum []checksumDat
	dm.lock.RLock()
	for name, fs := range dm.mp {
		if fs.Checksum == "" {
			needsChecksum = append(needsChecksum, checksumDat{Fname: name, fs: fs})
		}
	}
	dm.lock.RUnlock()
	if len(needsChecksum) == 0 {
		return nil
	}

	// Phase 2: Calculate checksums and update map (write lock needed for modifications)
	updates := make(map[Fname]FileStruct)
	type wkUnitResult struct {
		name Fname
		fs   FileStruct
		err  error
	}

	resultChan := make(chan wkUnitResult, len(needsChecksum))
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(needsChecksum))
		for _, csd := range needsChecksum {
			calcCs := func() error {
				defer wg.Done()
				name := csd.Fname
				fs := csd.fs
				log.Println("[DM] Calculating  checksum for", fs.Directory(), fs.Name)
				if workTokens != nil {
					<-workTokens
					defer func() { workTokens <- struct{}{} }()
				}
				cks, err := CalcMd5File(string(fs.directory), string(fs.Name))
				if err != nil {
					if !ignoreErrors {
						resultChan <- wkUnitResult{name: name, err: fmt.Errorf("checksum calculation error for %s/%s: %w", fs.Directory(), fs.Name, err)}
					}
					return nil
				}
				fs.Checksum = cks
				resultChan <- wkUnitResult{name: name, fs: fs}
				return nil
			}
			if dm.workerPool != nil {
				dm.workerPool.Submit(calcCs)
			} else {
				go calcCs()
			}
		}
		wg.Wait()
		close(resultChan)
	}()

	var errSaved error
	for wkUnitResult := range resultChan {
		if wkUnitResult.err != nil {
			errSaved = wkUnitResult.err
		}
		updates[wkUnitResult.name] = wkUnitResult.fs
	}
	if errSaved != nil {
		return errSaved
	}

	// Apply updates with write lock
	if len(updates) > 0 {
		dm.lock.Lock()
		maps.Copy(dm.mp, updates)
		*dm.stale = true
		dm.lock.Unlock()
	}
	return nil
}

// Stale returns true if the dm has been modified since writted
func (dm DirectoryMap) Stale() bool {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return *dm.stale
}

type checksumDat = struct {
	Fname Fname
	fs    FileStruct
}

// ForEachFile iterates over all files in the directory map
// The callback receives the filename and file metadata
// This guarantees that all returned metadata have valid checksums before the visitor is called
func (dm DirectoryMap) ForEachFile(fn ForEachCallback) error {
	err := dm.ChecksumCalc(nil)
	if err != nil {
		return err
	}

	// Phase 3: Visit files with guaranteed valid checksums (read-only)
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	for name, fs := range dm.mp {
		fsCopy := fs // Create a copy to avoid pointer issues
		fi := dm.fi[name]

		if err := fn(name, &fsCopy, fi); err != nil {
			return err
		}
	}
	return nil
}

var (
	// ErrDeleteThisEntry sentinel error to request deletion of an entry during mutation
	ErrDeleteThisEntry = errors.New("please delete this entry - thank you kindly")
	// ErrIgnoreThisMutate sentinel error to skip mutation of an entry
	ErrIgnoreThisMutate = errors.New("do not mutate this entry")
)

// RangeMutate range over the map, mutating as needed
// note one may return specific errors to delete or squash the mutation
func (dm DirectoryMap) RangeMutate(fc DmMutCallback) error {
	// Use injected pool if available, otherwise create temporary
	pool := dm.pool
	var tempPool *mutatePool
	if pool == nil {
		tempPool = newMutatePool()
		pool = tempPool
		defer tempPool.close()
	}
	deleteList := []Fname{}
	needsUpdate := func(current, updated FileStruct) bool {
		if current.Name != updated.Name {
			return true
		}
		if current.Mtime != updated.Mtime || current.Size != updated.Size || current.Checksum != updated.Checksum {
			return true
		}
		if current.Directory() != updated.Directory() {
			return true
		}
		if !slices.Equal(current.BackupDest, updated.BackupDest) {
			return true
		}
		if !slices.Equal(current.Tags, updated.Tags) {
			return true
		}
		return false
	}

	// Snapshot work items while holding the lock to keep map stable
	dm.lock.Lock()
	defer dm.lock.Unlock()
	items := make([]mutateWorkItem, 0, len(dm.mp))
	for fn, v := range dm.mp {
		fi, ok := dm.fi[fn]
		if !ok {
			// FIXME this should do a lookup....
			fi = nil
		}
		dir := v.Directory()
		if dir == "" && dm.dir != "" {
			dir = dm.dir
			v.SetDirectory(dm.dir)
		}
		items = append(items, mutateWorkItem{
			fn:       fn,
			fpath:    NewFpath(dir, fn),
			fi:       fi,
			fs:       v,
			callback: fc,
			resultCh: make(chan mutateResult, 1),
		})
	}
	// Keep the map locked for the whole operation to preserve existing semantics
	// (RangeMutate has always been a write-locked, serial operation on the map).
	// Workers do not touch the map directly, only the callback logic, so the lock
	// prevents external writers without blocking internal parallel work.
	for i := range items {
		pool.workCh <- items[i]
	}

	for _, item := range items {
		res := <-item.resultCh
		if res.err == nil && res.fs.Directory() == "" && item.fpath.Dir() != "" {
			res.fs.SetDirectory(item.fpath.Dir())
		}
		switch res.err {
		case nil:
			current := dm.mp[res.fn]
			if !needsUpdate(current, res.fs) {
				continue
			}
			dm.mp[res.fn] = res.fs
			*dm.stale = true
		case ErrIgnoreThisMutate:
		case ErrDeleteThisEntry:
			deleteList = append(deleteList, res.fn)
		default:
			return res.err
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
func (dm DirectoryMap) RunFsFc(path Fpath, fc FsFunc) error {
	fs, ok := dm.Get(path.Base())
	var err error
	if !ok {
		fs, err = NewFileStruct(string(path.Dir()), string(path.Base()))
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

// DeleteMissingFiles deletes any file entries that are in the dm,
// but not on the disk
func (dm DirectoryMap) DeleteMissingFiles() error {
	fc := func(file Fpath, d os.FileInfo, fs FileStruct) (FileStruct, error) {
		fp := filepath.Join(string(fs.directory), string(file.Base()))
		_, err := os.Stat(fp)
		if errors.Is(err, os.ErrNotExist) {
			return fs, ErrDeleteThisEntry
		}
		return fs, ErrIgnoreThisMutate
	}
	return dm.RangeMutate(fc)
}

// Persist self to disk
func (dm DirectoryMap) Persist(directory Dirname) error {
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
func (dm DirectoryMap) Visitor(path Fpath, d fs.DirEntry) error {
	// Note the difference between this and VisitFunc
	// We pass self in, so that the worker func can be declared once
	// rather than always having to be a closure
	// This is slightly odd, but requires fewer closures - and the costs associated
	dm.lock.RLock()
	visitFunc := dm.visitFunc
	dm.lock.RUnlock()
	return visitFunc(dm, path, d)
}

// UpdateValues in the DirectoryEntry to those found on the fs
func (dm DirectoryMap) UpdateValues(directory Dirname, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		// If the file no longer exists (e.g., moved/deleted after ReadDir but before Info),
		// silently ignore it rather than propagating the error
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	file := d.Name()
	dm.lock.Lock()
	dm.fi[Fname(file)] = info
	dm.lock.Unlock()
	fs, ok := dm.Get(Fname(file))
	if changed, err := fs.Changed(info); ok && !changed {
		return err
	}
	_, err = fs.FromStat(directory, Fname(file), info)
	if err != nil {
		return err
	}
	dm.Add(fs)
	return nil
}

// func (dm DirectoryMap) Copy() DirectoryMapJournalableInterface {
// 	cp := newDirectoryMap()
// 	dm.lock.RLock()
// 	maps.Copy(cp.mp, dm.mp)
// 	visitFunc := dm.visitFunc
// 	dm.lock.RUnlock()
// 	cp.lock.Lock()
// 	cp.visitFunc = visitFunc
// 	cp.lock.Unlock()
// 	return cp
// }

func (dm0 DirectoryMap) Equal(dm1 DirectoryMap) bool {
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
