package medorg

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var Debug bool

const Md5FileName = ".md5_list.xml"
const idleWriteDuration = 30 * time.Second

type FileStruct struct {
	XMLName   struct{} `xml:"fr"`
	directory string   // Kept as hidden from the xml as this is used for messaging between agents
	// and that does not want to end up in the final xml file
	Name     string `xml:"fname,attr"`
	Checksum string `xml:"checksum,attr"`

	Mtime int64    `xml:"mtime,attr,omitempty"`
	Size  int64    `xml:"size,attr,omitempty"`
	Tags  []string `xml:"tags,omitempty"`
}

func (fs FileStruct) String() string {
	retStr := "[FileStruct]{"
	if fs.directory != "" {
		retStr += "directory:\"" + fs.directory + "\""
	}
	retStr += "Name:\"" + fs.Name + "\""
	retStr += "Checksum:" + fs.Checksum + "\""
	retStr += "}"

	return retStr
}
func (fs FileStruct) Directory() string {
	return fs.directory
}
func FsFromName(directory, fn string) FileStruct {
	fp := directory + "/" + fn
	fs, err := os.Stat(fp)

	if os.IsNotExist(err) {
		log.Fatal("Asked to create a fs for a file that does not exist", fp)
	}

	itm := new(FileStruct)
	itm.Name = fn
	itm.Mtime = fs.ModTime().Unix()
	itm.Size = fs.Size()
	itm.directory = directory
	if Debug {
		log.Println("New FS for file", fp, "Size:", itm.Size, " Time:", itm.Mtime)
	}
	return *itm
}

type Md5File struct {
	XMLName struct{}     `xml:"dr"`
	Files   []FileStruct `xml:"fr"`
}
type DirectoryMap struct {
	mp    map[string]FileStruct
	stale *bool
	lock  *sync.RWMutex
}

func NewDirectoryMap() *DirectoryMap {
	itm := new(DirectoryMap)
	itm.mp = make(map[string]FileStruct)
	itm.stale = new(bool)
	itm.lock = new(sync.RWMutex)
	return itm
}
func (dm DirectoryMap) MarshalXml() (output []byte, err error) {
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
	return m5f.MarshalXml()
}
func (dm *DirectoryMap) UnmarshalXml(input []byte) (err error) {
	var m5f Md5File
	err = m5f.UnmarshalXml(input)
	if err != nil {
		return err
	}
	for _, val := range m5f.Files {
		dm.Add(val)
	}
	return nil
}
func (dm DirectoryMap) Add(fs FileStruct) {
	dm.lock.Lock()
	fn := fs.Name
	dm.mp[fn] = fs
	*dm.stale = true
	dm.lock.Unlock()
}
func (dm DirectoryMap) Rm(fn string) {
	dm.lock.Lock()
	dm.rm(fn)
	dm.lock.Unlock()
}
func (dm DirectoryMap) rm(fn string) {
	delete(dm.mp, fn)
}
func (dm DirectoryMap) Get(fn string) (FileStruct, bool) {
	dm.lock.RLock()
	fs, ok := dm.mp[fn]
	dm.lock.RUnlock()
	return fs, ok
}
func NewMd5File() *Md5File {
	itm := new(Md5File)
	itm.Files = make([]FileStruct, 0)
	return itm
}
func (md *Md5File) Append(fs FileStruct) {
	md.Files = append(md.Files, fs)
}
func (md *Md5File) AddFile(filename string) {
	md.Append(FileStruct{Name: filename})
	// TBD addin code to start the checksum process
}
func (md Md5File) MarshalXml() (output []byte, err error) {
	//output, err = xml.Marshal(md)

	output, err = xml.MarshalIndent(md, "", "  ")
	return
}
func (md Md5File) String() string {
	txt, err := xml.MarshalIndent(md, "", "  ")
	switch err {
	case nil:
	case io.EOF:
	default:
		log.Fatal("Unknown Error Marshalling Md5File:", err)
	}
	return string(txt)
}
func (md *Md5File) UnmarshalXml(input []byte) (err error) {
	err = xml.Unmarshal(input, md)
	//fmt.Printf("Unmarshalling completed on:\n%v\nOutput:\n%v\n\n",input, md)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		log.Fatal("Unknown Error UnMarshalling Md5File:", err)
	}
	return
}
func NewChannels() (input_chan chan FileStruct, output_chan chan FileStruct, closed_chan chan struct{}) {
	input_chan = make(chan FileStruct)
	output_chan = make(chan FileStruct)
	closed_chan = make(chan struct{})
	go md5Calcer(input_chan, output_chan, closed_chan)
	return
}

func md5Calcer(input_chan chan FileStruct, output_chan chan FileStruct, closed_chan chan struct{}) {
	for itm := range input_chan {
		// Calculate the MD5 here and send it
		fn := itm.Name
		log.Println("Received fn")
		// TBD send this to a cetral engine to calculate many in parallel
		cks, err := CalcMd5File(".", fn)
		if err != nil {
			log.Fatal("Calculation error", err)
		} else {
			log.Println("Calculation for", fn, " complete")
		}
		itm.Checksum = cks
		output_chan <- itm
	}
	log.Println("md5Calcer closed")
	close(output_chan)
	close(closed_chan)
}

// NewXmlManager creates a new file manager
// This receives FileStructs and stroes those contents in
// an appropriate .md5_file.xml
func NewXmlManager(input_chan chan FileStruct) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go managerWorker(input_chan, &wg)
	return &wg
}

func managerWorker(input_chan chan FileStruct, wg *sync.WaitGroup) {
	for fs := range input_chan {
		direct := fs.directory
		if direct == "" {
			log.Fatal("Empty Directory description")
		}
		// TBD create a local cache and flush mechanism here
		appendXml(direct, []FileStruct{fs})
	}
	log.Println("managerWorker closing")
	wg.Done()
}
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
func DirectoryMapFromDir(directory string) DirectoryMap {
	// Read in the xml structure to a map/array
	var dm DirectoryMap
	dm = *NewDirectoryMap()
	if dm.mp == nil {
		log.Fatal("Initialize malfunction!")
	}
	fn := directory + "/" + Md5FileName
	var f *os.File
	_, err := os.Stat(fn)

	if !os.IsNotExist(err) {
		f, err = os.Open(fn)

		if err != nil {
			log.Fatalf("error opening file: %T,%v\n", err, err)
		}
		byteValue, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			log.Fatalf("error loading file: %T,%v\n", err, err)
		}

		dm.UnmarshalXml(byteValue)
		//fmt.Printf("******\n%v\n*****%v\n****\n", dm, fileContents)
		if dm.mp == nil {
			log.Fatal("Learn to code Chris")
		}
		dm.PopulateDirectory(directory)
	}
	return dm
}
func removeMd5(directory string) {
	fn := directory + "/" + Md5FileName
	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		os.Remove(fn)
	}
}

// appendXml - append items to the existing Xml File
func appendXml(directory string, fsA []FileStruct) {
	dm := DirectoryMapFromDir(directory)
	// Add in the items in the input
	for _, fs := range fsA {
		// Check each item to make sure it matches the current directory
		if fs.directory == directory {
			dm.Add(fs)
		} else {
			log.Fatal("directories are incorrect", fs.directory, directory)
		}
	}
	dm.WriteDirectory(directory)
}
func (dm DirectoryMap) Len() int {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return len(dm.mp)
}
func (dm DirectoryMap) Stale() bool {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	return *dm.stale
}
func (dm DirectoryMap) WriteDirectory(directory string) {
	if !dm.Stale() {
		return
	}
	*dm.stale = false
	if dm.Len() == 0 {
		removeMd5(directory)
		return
	}
	// Write out a new Xml from the structure
	ba, err := dm.MarshalXml()
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

// reduceXml - read in the xml
// and reduce it to the items that still exist on the fs
func reduceXml(directory string) {
	// Read in xml
	// Write out Xml if changed

}
func (dm DirectoryMap) Range(fc func(string, FileStruct)) {
	dm.lock.RLock()
	for local_fn, v := range dm.mp {
		fc(local_fn, v)
	}
	dm.lock.RUnlock()
}
func (dm DirectoryMap) Deleter(fc func(string, FileStruct) bool) {
	delList := make([]string, 0)

	dm.lock.RLock()
	for local_fn, v := range dm.mp {
		toDelete := fc(local_fn, v)
		if toDelete {
			delList = append(delList, local_fn)
		}
	}
	dm.lock.RUnlock()
	if len(delList) > 0 {
		dm.lock.Lock()
		for local_fn, _ := range dm.mp {
			dm.rm(local_fn)
		}
		dm.lock.Unlock()
	}
}
func RmFile(dir, fn string) error {
	dm := DirectoryMapFromDir(dir)
	return dm.RmFile(dir, fn)
}
func (dm DirectoryMap) RmFile(dir, fn string) error {
	dm.Rm(fn)
	dm.WriteDirectory(dir)
	return RemoveFile(dir + "/" + fn)

}
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

// reduceXmlFe is the front end of reduceXml
// it reads in, performs the reduction and returns a file struct
// that only contains files that exist
// If the file attributes have changed then that counts
// as not existing
func reduceXmlFe(directory string) DirectoryMap {
	// Read in the current file
	// if it exists
	dm := DirectoryMapFromDir(directory)
	//log.Printf("\n\n%s\n*****\n%v\n\n\n\n",directory,dm)
	theFunc := func(local_fn string, v FileStruct) bool {
		fn := directory + "/" + local_fn
		if local_fn == "" {
			if Debug {
				log.Println("Blank filename in xml", fn)
			}
			return true
		}
		// for each file, check if it exists
		if fstat, err := os.Stat(fn); os.IsNotExist(err) {
			// if it does not, remove from the map
			return true
		} else if os.IsExist(err) || (err == nil) {
			// If it does, then check if the attributes are accurate
			ftD := fstat.ModTime().Unix()
			szD := fstat.Size()
			ftX := v.Mtime
			szX := v.Size

			if ftD != ftX {
				log.Println("File times for ", fn, "do not match. File:", ftD, "Xml:", ftX)
				return true
			}
			if szD != szX {
				log.Println("Sizes for ", fn, "do not match. File:", szD, "Xml:", szX)
				return true
			}
			return false
		} else {
			log.Fatal("A file that neither exists, nor doesn't exist", err)
		}
		return false
	}
	dm.Deleter(theFunc)

	// Return the structure we have created as it is useful
	return dm
}

type TreeUpdate struct {
	walkerToken chan struct{}
	calcToken   chan struct{}
	closeChan   chan struct{}
	pendToken   chan struct{}

	wg *sync.WaitGroup
}

// Our Worker will allow up to items to be issued as tokens
func (tu TreeUpdate) worker(items int, ch chan struct{}) {
	var outstandingTokens int
	var writeChan chan struct{}
	if items > 0 {
		writeChan = ch
	}
	var closed bool

	for !closed || (outstandingTokens > 0) {
		select {
		case <-tu.closeChan:
			closed = true
			writeChan = nil
		case writeChan <- struct{}{}:
			outstandingTokens++
			if outstandingTokens >= items {
				writeChan = nil
			}
		case <-ch:
			if outstandingTokens <= 0 {
				log.Fatal("Negative tokens")
			}
			outstandingTokens--
			if !closed {
				writeChan = ch
			}
		}
	}
	tu.wg.Done()
}

func NewTreeUpdate(walkCount, calcCount, pendCount int) (tu TreeUpdate) {
	tu.walkerToken = make(chan struct{})
	tu.calcToken = make(chan struct{})
	tu.pendToken = make(chan struct{})
	tu.closeChan = make(chan struct{})
	tu.wg = new(sync.WaitGroup)
	tu.wg.Add(3)
	go tu.worker(walkCount, tu.walkerToken)
	go tu.worker(calcCount, tu.calcToken)
	go tu.worker(pendCount, tu.pendToken)
	return
}

type ModifyFunc func(dir, fn string, fs FileStruct) (FileStruct, bool)
type CalcingFunc func(dir, fn string) (string, error)
type WalkingFunc func(dir string, wkf WalkingFunc)

func (tu TreeUpdate) Close() {
	close(tu.closeChan)
	tu.wg.Wait()
}
func (tu TreeUpdate) UpdateDirectory(directory string, mf ModifyFunc) {

	tmpFunc := func(dir, fn string) (string, error) {
		if Debug {
			log.Println("Attempting to get cal token for:", dir, "/", fn)
		}
		<-tu.calcToken
		defer func() {
			tu.calcToken <- struct{}{}
		}()
		return CalcMd5File(dir, fn)
	}

	walkFunc := func(dir string, wkf WalkingFunc) {
		if Debug {
			log.Println("Attemping to get walk token for:", dir)
		}
		updateDirectory(dir, tmpFunc, wkf, tu.pendToken, tu.walkerToken, mf)
	}
	<-tu.walkerToken
	walkFunc(directory, walkFunc)
	tu.walkerToken <- struct{}{}
	tu.Close()
}

// UpdateDirectory will for a specified directory
// go through and update the xml file
func UpdateDirectory(directory string, mf ModifyFunc) {
	dirCnt := 4
	pendCnt := 4
	dirToken := make(chan struct{}, dirCnt)
	for i := 0; i < dirCnt; i++ {
		dirToken <- struct{}{}
	}
	pendToken := make(chan struct{}, pendCnt)
	for i := 0; i < pendCnt; i++ {
		pendToken <- struct{}{}
	}

	tmpFunc := func(directory, fn string) (string, error) {
		return CalcMd5File(directory, fn)
	}

	walkFunc := func(dir string, wkf WalkingFunc) {
		<-dirToken
		updateDirectory(dir, tmpFunc, wkf, pendToken, dirToken, mf)
		dirToken <- struct{}{}
	}

	walkFunc(directory, walkFunc)
}
func (dm *DirectoryMap) idleWriter(closeChan chan struct{}, directory string) *sync.WaitGroup {
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
func updateDirectory(directory string, calcFunc CalcingFunc, walkFunc WalkingFunc, pendTok, dirTok chan struct{}, mf ModifyFunc) {
	var dwg sync.WaitGroup
	var fwg sync.WaitGroup
	closeChan := make(chan struct{})
	//<-dirTok
	// Reduce the xml to only the items that exist
	dm := reduceXmlFe(directory)

	// Now read in all files in the current directory
	stats, err := ioutil.ReadDir(directory)

	if err != nil {
		log.Fatal(err)
	}
	dirTok <- struct{}{}
	writerWg := dm.idleWriter(closeChan, directory)

	// Put the token back so we will always be able to
	// recurse at least once
	for _, file := range stats {
		fn := file.Name()
		if strings.HasPrefix(fn, ".") {
			// Don't build for hidden files
			continue
			// If it is a directory, then go into it
		}
		if file.IsDir() {
			log.Println("Going into directory:", fn)
			dwg.Add(1)
			<-dirTok
			go func() {
				walkFunc(directory+"/"+fn, walkFunc)
				dwg.Done()
				log.Println("Finished with directory:", fn)
				dirTok <- struct{}{}
			}()
		} else {
			var fs FileStruct
			fs, ok := dm.Get(fn)

			if ok {
				if mf != nil {
					fs, update := mf(directory, fn, fs)
					if update {
						dm.Add(fs)
					}
				}
			} else {
				<-pendTok
				fwg.Add(1)
				go func() {
					fs = FsFromName(directory, fn)
					cs, err := calcFunc(directory, fn)
					pendTok <- struct{}{}
					if err == nil {
						fs.Checksum = cs
						if mf != nil {
							fsLocal, update := mf(directory, fn, fs)
							if update {
								fs = fsLocal
							}
						}
						dm.Add(fs)
					} else {
						log.Fatal("Error back from checksum calculation", err)
					}
					fwg.Done()
				}()
			}
		}
	}
	// we've done with using IO ourselves
	// so allow someone else to
	dwg.Wait()
	fwg.Wait()
	// Now the md struct is up to date
	// write it out
	close(closeChan)
	writerWg.Wait()
	// retrieve the token we're expected to have
	<-dirTok
}
func CalcMd5File(directory, fn string) (string, error) {
	fp := directory + "/" + fn
	fmt.Println("Calculating Checksum for", fp)
	f, err := os.Open(fp)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	//retStr := fmt.Sprintf("%x", h.Sum(nil))
	retStr := base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(h.Sum(nil)))
	return retStr, nil
}

type TreeWalker struct{}

func NewTreeWalker() *TreeWalker {
	itm := new(TreeWalker)
	return itm
}

// WalkFunc can modify the dm it is passed
// if one does this, you must return true
type WalkFunc func(directory, fn string, fs FileStruct, dm *DirectoryMap) bool

// DirectFunc is called at the end of walking each directory
type DirectFunc func(directory string, dm *DirectoryMap)

func (tw TreeWalker) WalkTree(directory string, wf WalkFunc, df DirectFunc) {
	dm := DirectoryMapFromDir(directory)
	// Now read in all files in the current directory
	stats, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	var update bool

	// Put the token back so we will always be able to
	// recurse at least once
	for _, file := range stats {
		fn := file.Name()
		if strings.HasPrefix(fn, ".") {
			// Don't build for hidden files
			continue
		}
		// If it is a directory, then go into it
		if file.IsDir() {
			log.Println("Going into directory:", directory)
			tw.WalkTree(directory+"/"+fn, wf, df)
			log.Println("Finished with directory:", directory)
		} else {
			var fs FileStruct
			fs, ok := dm.Get(fn)

			if ok {
				if wf != nil {
					// Annoying syntax to ensure the worker function
					// always gets run
					updateTmp := wf(directory, fn, fs, &dm)
					update = update || updateTmp
				}
			} else {
				log.Fatal("This should not be possible after UpdateDirectory", directory, fn)
			}
		}
	}
	if update {
		dm.WriteDirectory(directory)
	}
	if df != nil {
		df(directory, &dm)
	}
}
