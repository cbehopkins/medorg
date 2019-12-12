package medorg

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

// DirectoryMap contains for the directory all the file structs
type DirectoryMap struct {
	mp    map[string]FileStruct
	stale *bool
	lock  *sync.RWMutex
}

// NewDirectoryMap creates a new dm
func NewDirectoryMap() *DirectoryMap {
	itm := new(DirectoryMap)
	itm.mp = make(map[string]FileStruct)
	itm.stale = new(bool)
	itm.lock = new(sync.RWMutex)
	return itm
}

//ToXML is a standard marshaller
func (dm DirectoryMap) ToXML() (output []byte, err error) {
	m5f := NewMd5File()
	dm.lock.RLock()
	for key, value := range dm.mp {
		if key == value.Name {
			m5f.Append(value)
		} else {
			log.Fatal("KV not match")
		}
	}
	dm.lock.RUnlock()
	return m5f.ToXML()
}

// FromXML is a standard unmarshaller
func (dm *DirectoryMap) FromXML(input []byte) (err error) {
	var m5f Md5File
	err = m5f.FromXML(input)
	if err != nil {
		return err
	}
	for _, val := range m5f.Files {
		dm.Add(val)
	}
	return nil
}

// Add adds a file struct to the dm
func (dm *DirectoryMap) Add(fs FileStruct) {
	dm.lock.Lock()
	fn := fs.Name
	dm.mp[fn] = fs
	*dm.stale = true
	dm.lock.Unlock()
}

// Rm Removes a filename from the dm
func (dm DirectoryMap) Rm(fn string) {
	dm.lock.Lock()
	dm.rm(fn)
	dm.lock.Unlock()
}
func (dm DirectoryMap) rm(fn string) {
	delete(dm.mp, fn)
}

// RmFile is similar to rm, but updates the directory
func (dm DirectoryMap) RmFile(dir, fn string) error {
	dm.Rm(fn)
	dm.WriteDirectory(dir)
	return RemoveFile(dir + "/" + fn)

}

// Get the struct associated with a filename
func (dm DirectoryMap) Get(fn string) (FileStruct, bool) {
	dm.lock.RLock()
	fs, ok := dm.mp[fn]
	dm.lock.RUnlock()
	return fs, ok
}

// PopulateDirectory the directory strings of the structs
// useful after reading on the xml
func (dm DirectoryMap) PopulateDirectory(directory string) {
	dm.lock.Lock()
	for fn, fs := range dm.mp {
		fs.directory = directory
		dm.mp[fn] = fs
	}
	dm.lock.Unlock()
}

// DirectoryMapFromDir reads in the dirmap from the supplied dir
// It does not check anything or compute anythiing
func DirectoryMapFromDir(directory string) (dm DirectoryMap) {
	// Read in the xml structure to a map/array
	dm = *NewDirectoryMap()
	if dm.mp == nil {
		log.Fatal("Initialize malfunction!")
	}
	fn := directory + "/" + Md5FileName
	var f *os.File
	_, err := os.Stat(fn)

	if os.IsNotExist(err) {
		return
	}
	f, err = os.Open(fn)

	if err != nil {
		log.Printf("error opening directory map file: %T,%v\n", err, err)
		log.Println("Which is odd as:", os.IsNotExist(err), err)
		_, err := os.Stat(fn)
		log.Println("and:", os.IsNotExist(err), err)
		return
	}
	byteValue, err := ioutil.ReadAll(f)
	_ = f.Close()
	if err != nil {
		log.Fatalf("error loading file: %T,%v\n", err, err)
	}

	err = dm.FromXML(byteValue)
	if err != nil {
		return
	}
	//fmt.Printf("******\n%v\n*****%v\n****\n", dm, fileContents)
	if dm.mp == nil {
		log.Fatal("Learn to code Chris")
	}
	dm.PopulateDirectory(directory)
	dm.SelfCheck(directory)
	return
}

// Len is how many items in the dm
func (dm DirectoryMap) Len() int {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return len(dm.mp)
}

// Stale returns true if the dm has been modified since writted
func (dm DirectoryMap) Stale() bool {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return *dm.stale
}

func (dm DirectoryMap) SelfCheck(directory string) {
	fc := func(fn string, fs FileStruct) {
		if fs.Directory() != directory {
			log.Fatal("Self check problem. FS has directory of ", fs.Directory(), " for ", directory)
		}
	}
	dm.Range(fc)

}

// WriteDirectory writes the dm out to the directory specified
func (dm DirectoryMap) WriteDirectory(directory string) {
	dm.SelfCheck(directory)
	if !dm.Stale() {
		return
	}
	*dm.stale = false
	if dm.Len() == 0 {
		removeMd5(directory)
		return
	}
	// Write out a new Xml from the structure
	ba, err := dm.ToXML()
	switch err {
	case nil:
	case io.EOF:
	default:
		log.Fatal("Unknown Error Marshalling Xml:", err)
	}
	fn := directory + "/" + Md5FileName
	removeMd5(directory)
	err = ioutil.WriteFile(fn, ba, 0600)
	if err != nil {
		log.Fatal(err)
	}
}

// Range over all the items in the map
func (dm DirectoryMap) Range(fc func(string, FileStruct)) {
	dm.lock.RLock()
	for fn, v := range dm.mp {
		fc(fn, v)
	}
	dm.lock.RUnlock()
}

// Deleter deletes the supplied struct if the function returns true
func (dm DirectoryMap) Deleter(fc func(string, FileStruct) bool) {
	delList := make([]string, 0)

	dm.lock.RLock()
	for fn, v := range dm.mp {
		toDelete := fc(fn, v)
		if toDelete {
			delList = append(delList, fn)
		}
	}
	dm.lock.RUnlock()
	if len(delList) > 0 {
		dm.lock.Lock()
		for _, fn := range delList {
			dm.rm(fn)
		}
		dm.lock.Unlock()
	}
}

// reduceXMLFe is the front end of reduceXml
// it reads in, performs the reduction and returns a file struct
// that only contains files that exist
// If the file attributes have changed then that counts
// as not existing
func reduceXMLFe(directory string) DirectoryMap {
	// Read in the current file
	// if it exists
	dm := DirectoryMapFromDir(directory)
	//log.Printf("\n\n%s\n*****\n%v\n\n\n\n",directory,dm)
	theFunc := func(fn string, v FileStruct) bool {
		return v.checkDelete(directory, fn)
	}
	dm.Deleter(theFunc)

	// Return the structure we have created as it is useful
	return dm
}
func (dm DirectoryMap) idleWriter(closeChan chan struct{}, directory string) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		var closed bool
		for !closed {
			select {
			case <-closeChan:
				dm.WriteDirectory(directory)
				closed = true
			case <-time.After(idleWriteDuration):
				dm.WriteDirectory(directory)
			}
		}
		wg.Done()
	}()
	return &wg
}
