package medorg

import (
	"bufio"
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

const Md5FileName = ".md5_list.xml"
const idleWriteDuration = 30 * time.Second

type FileStruct struct {
	XMLName   struct{} `xml:"fr"`
	directory string   // Kept as hidden from the xml as this is used for messaging between agents
	// and that does not want to end up in the final xml file
	delete   bool   // Should this file be deleted from the structure
	Name     string `xml:"fname,attr"`
	Checksum string `xml:"checksum,attr"`

	Mtime int64    `xml:"mtime,attr,omitempty"`
	Size  int64    `xml:"size,attr,omitempty"`
	Tags  []string `xml:"tags,omitempty"`
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
	log.Println("New FS for file", fp, "Size:", itm.Size, " Time:", itm.Mtime)
	return *itm
}

type Md5File struct {
	XMLName struct{}     `xml:"dr"`
	Files   []FileStruct `xml:"fr"`
}
type DirectoryMap map[string]FileStruct

func NewDirectoryMap() *DirectoryMap {
	itm := new(DirectoryMap)
	*itm = make(DirectoryMap)
	return itm
}
func (dm DirectoryMap) MarshalXml() (output []byte, err error) {
	m5f := NewMd5File()
	for key, value := range dm {
		if key == value.Name {
			m5f.Append(value)
		} else {
			log.Fatal("KV not match")
		}
	}
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
	fn := fs.Name
	dm[fn] = fs
}
func (dm DirectoryMap) Get(fn string) (FileStruct, bool) {
	fs, ok := dm[fn]
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

// DirectoryMapFromDir reads in the dirmap from the supplied dir
// It does not check anything or compute anythiing
func DirectoryMapFromDir(directory string) DirectoryMap {
	// Read in the xml structure to a map/array
	var dm DirectoryMap
	dm = *NewDirectoryMap()
	if dm == nil {
		log.Fatal("Initialize malfunction!")
	}
	fn := directory + "/" + Md5FileName
	var f *os.File
	_, err := os.Stat(fn)

	if os.IsNotExist(err) {
		f, err = os.Create(fn)
		if err != nil {
			log.Fatalf("error creating file: %T,%v\n", err, err)
		}
		if dm == nil {
			log.Fatal("Learn to code Chris Init didn't work")
		}
	} else {
		f, err = os.Open(fn)

		if err != nil {
			log.Fatalf("error opening file: %T,%v\n", err, err)
		}
		//var fileContents string
		//r := bufio.NewReader(f)
		//for s, e := Readln(r); e == nil; s, e = Readln(r) {
		//	fileContents += s
		//}
		byteValue, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			log.Fatalf("error loading file: %T,%v\n", err, err)
		}

		dm.UnmarshalXml(byteValue)
		//fmt.Printf("******\n%v\n*****%v\n****\n", dm, fileContents)
		if dm == nil {
			log.Fatal("Learn to code Chris")
		}
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
func (dm DirectoryMap) WriteDirectory(directory string) {
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
	if false {
		f, err := os.Create(fn)
		if err != nil {
			log.Fatal("Error creating ", fn)
		}
		//fmt.Println("XML out is:", string(ba))
		fmt.Fprintf(f, string(ba))

		f.Sync()
		f.Close()
	} else {
		err := ioutil.WriteFile(fn, ba, 0600)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// reduceXml - read in the xml
// and reduce it to the items that still exist on the fs
func reduceXml(directory string) {
	// Read in xml
	// Write out Xml if changed

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
	toDelete := make([]string, 0)
	for local_fn, v := range dm {
		fn := directory + "/" + local_fn
		// for each file, check if it exists
		if fstat, err := os.Stat(fn); os.IsNotExist(err) {
			// if it does not, remove from the map
			toDelete = append(toDelete, fn)
		} else if os.IsExist(err) || (err == nil) {
			// If it does, then check if the attributes are accurate
			ftD := fstat.ModTime().Unix()
			szD := fstat.Size()
			ftX := v.Mtime
			szX := v.Size

			if ftD != ftX {
				log.Println("File times for ", fn, "do not match. File:", ftD, "Xml:", ftX)
				toDelete = append(toDelete, fn)
				continue
			}
			if szD != szX {
				log.Println("Sizes for ", fn, "do not match. File:", szD, "Xml:", szX)
				toDelete = append(toDelete, fn)
				continue
			}
		} else {
			log.Fatal("A file that neither exists, nor doesn't exist", err)
		}
	}
	for _, fn := range toDelete {
		delete(dm, fn)
	}

	// Return the structure we have created as it is useful
	return dm
}

type TreeUpdate struct {
	walkerToken chan struct{}
	calcToken   chan struct{}
	closeChan   chan struct{}
	wg          *sync.WaitGroup
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
func NewTreeUpdate(walkCount, calcCount int) (tu TreeUpdate) {
	tu.walkerToken = make(chan struct{}, walkCount)
	tu.calcToken = make(chan struct{}, calcCount)
	tu.closeChan = make(chan struct{})
	tu.wg = new(sync.WaitGroup)
	tu.wg.Add(2)
	//for i := 0; i < walkCount; i++ {
	//	tu.walkerToken <- struct{}{}
	//}
	//for i := 0; i < calcCount; i++ {
	//	tu.calcToken <- struct{}{}
	//}
	go tu.worker(walkCount, tu.walkerToken)
	go tu.worker(calcCount, tu.calcToken)
	return
}

type CalcingFunc func(dir, fn string) (string, error)
type WalkingFunc func(dir string, wkf WalkingFunc)

func (tu TreeUpdate) Close() {
	close(tu.closeChan)
	tu.wg.Wait()
}
func (tu TreeUpdate) UpdateDirectory(directory string) {
	tmpFunc := func(dir, fn string) (string, error) {
		log.Println("Attempting to get cal token for:", dir, "/", fn)
		<-tu.calcToken
		defer func() {
			tu.calcToken <- struct{}{}
		}()
		return CalcMd5File(dir, fn)
	}

	walkFunc := func(dir string, wkf WalkingFunc) {
		log.Println("Attemping to get walk token for:", dir)
		<-tu.walkerToken
		log.Println("Got Walk Token", dir)
		updateDirectory(dir, tmpFunc, wkf)
		tu.walkerToken <- struct{}{}
	}
	walkFunc(directory, walkFunc)
	tu.Close()
}

// UpdateDirectory will for a specified directory
// go through and update the xml file
func UpdateDirectory(directory string) {
	tmpFunc := func(directory, fn string) (string, error) {
		return CalcMd5File(directory, fn)
	}

	walkFunc := func(dir string, wkf WalkingFunc) {
		updateDirectory(dir, tmpFunc, wkf)
	}

	walkFunc(directory, walkFunc)
}
func (dm *DirectoryMap) idleWriter(closeChan chan struct{}, locker *sync.Mutex, directory string) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
    var closed bool
		for !closed {
			select {
			case <-closeChan:
				locker.Lock()
				dm.WriteDirectory(directory)
				locker.Unlock()
        closed = true
			case <-time.After(idleWriteDuration):
				locker.Lock()
				dm.WriteDirectory(directory)
				locker.Unlock()
			}
		}
		wg.Done()
	}()
	return &wg
}
func updateDirectory(directory string, calcFunc CalcingFunc, walkFunc WalkingFunc) {
	var wg sync.WaitGroup
	closeChan := make(chan struct{})
	var dlk sync.Mutex
	// Reduce the xml to only the items that exist
	dm := reduceXmlFe(directory)
	writerWg := dm.idleWriter(closeChan, &dlk, directory)
	// Now read in all files in the current directory
	stats, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range stats {
		fn := file.Name()
		if strings.HasPrefix(fn, ".") {
			// Don't build for hidden files
			continue
			// If it is a directory, then go into it
		}
		if file.IsDir() {
			if true {
				log.Println("Going into directory:", fn)
				wg.Add(1)
				go func() {
					walkFunc(directory+"/"+fn, walkFunc)
					wg.Done()
				}()
			}
		} else {
			if _, ok := dm.Get(fn); !ok {
				wg.Add(1)
				go func() {
					fs := FsFromName(directory, fn)
					cs, err := calcFunc(directory, fn)
					if err == nil {
						fs.Checksum = cs
						dlk.Lock()
						dm.Add(fs)
						dlk.Unlock()
					} else {
						log.Fatal("Error back from checksum calculation", err)
					}
					wg.Done()
				}()
			}
		}
	}
	wg.Wait()
	close(closeChan)
	// Now the md struct is up to date
	// write it out
	writerWg.Wait()
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

// Readln read a line from a standard reader
// TBD remove the need for this
// It's inefficient copy and paste coding
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
