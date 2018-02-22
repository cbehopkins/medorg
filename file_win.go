package medorg

import (
	"io/ioutil"
	"log"
	"strconv"

	"github.com/icza/gowut/gwu"
)

// FlSt File Structure
type FlSt struct {
	gtab gwu.Table
	ltab [][]gwu.Label
	pan  gwu.Panel
	dir  string
}

func newInputCel(x, y int, tab gwu.Table) {
	tmpTextBox := gwu.NewTextBox("")
	tmpTextBox.SetMaxLength(1)
	tmpTextBox.Style().SetWidthPx(10)
	tmpTextBox.AddSyncOnETypes(gwu.ETypeKeyUp)
	tab.Add(tmpTextBox, y, x)
}
func (fs *FlSt) getLabelRow(y int) []gwu.Label {
	// TBD can we use cap here to save workload
	numRows := len(fs.ltab)
	if y < numRows {
		return fs.ltab[y]
	}
	tmpArray := make([][]gwu.Label, y+1)
	copy(tmpArray, fs.ltab)
	fs.ltab = tmpArray
	return fs.ltab[y]
}

// getLabelCell - making the cell if needed
func (fs *FlSt) getLabelCell(x, y int) gwu.Label {
	rw := fs.getLabelRow(y)
	numCols := len(rw)
	if x < numCols {
		return rw[x]
	}
	tmpArray := make([]gwu.Label, x+1)
	copy(tmpArray, rw)
	fs.ltab[y] = tmpArray
	return fs.ltab[y][x]
}
func (fs *FlSt) setLabelCell(x, y int, lb gwu.Label) {
	rw := fs.getLabelRow(y)
	numCols := len(rw)
	if x < numCols {
		fs.ltab[y][x] = lb
		return
	}
	tmpArray := make([]gwu.Label, x+1)
	copy(tmpArray, rw)
	fs.ltab[y] = tmpArray
	fs.ltab[y][x] = lb
	return
}
func (fs *FlSt) newOutputCel(x, y int) {
	var tab gwu.Table

	tmpLabel := gwu.NewLabel("_")
	tmpLabel.Style().SetWidthPx(10)
	tmpLabel.AddSyncOnETypes(gwu.ETypeKeyUp)
	fs.setLabelCell(x, y, tmpLabel)
	tab = fs.gtab
	tab.Add(tmpLabel, y, x)
}
func (fs *FlSt) setTxt(x, y int, str string) {
	fs.ltab[y][x].SetText(str)
}
func (fs *FlSt) setDir(x, y int) {
	fs.ltab[y][x].Style().SetColor(gwu.ClrRed)
}
func getFiles(dir string) []Stats {
	stats, err := ioutil.ReadDir(dir)
	//log.Println("Getting files for directory:",dir)
	var retA []Stats
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range stats {
		//log.Println("File called", file.Name())
		fS := Stats{Name: file.Name(), Directory: false, Exe: false}
		if file.IsDir() {
			fS.Directory = true
		} else {
		}
		retA = append(retA, fS)
	}
	return retA
}

// FileWin creates the window object that
// all the anagram if resides in
func FileWin(path string) gwu.Window {

	win := gwu.NewWindow("file", "File")
	win.Style().SetFullWidth()
	win.SetHAlign(gwu.HACenter)
	win.SetCellPadding(2)
	var prev gwu.Panel
	panelTb := newPanel(path, prev, myWindow{win})
	win.Add(panelTb)
	return win
}
func newPanel(path string, prev gwu.Panel, win myWindow) gwu.Panel {
	// A panel for each major thing
	panelTb := gwu.NewHorizontalPanel()
	button := gwu.NewButton("Parent")
	if prev == nil {
		prev = panelTb
	}
	listTable := popDirect(path, panelTb, prev, win)
	log.Printf("New Table for directory:%s\n%v\n", path, listTable)
	button.AddEHandler(&dirProcessHandler{win: win, dir: path, current: panelTb, parent: prev}, gwu.ETypeClick)
	log.Println("Creating Button with:", prev)
	panelTb.Add(listTable)
	panelTb.Add(button)
	return panelTb
}

// Populate the directory
func popDirect(path string, pan gwu.Panel, prev gwu.Panel, win myWindow) gwu.Table {
	listTable := gwu.NewTable()
	fs := FlSt{
		gtab: listTable,
		pan:  pan,
		dir:  path,
	}
	sts := getFiles(path)
	fs.popStatus(sts, prev, win)
	return listTable
}
func (fs *FlSt) buildTable(x, y int) {
	for i := 0; i < x; i++ {
		for j := 0; j < y; j++ {
			fs.newOutputCel(i, j)
		}
	}
}

// Populate the status
func (fs *FlSt) popStatus(stats []Stats, prev gwu.Panel, win myWindow) {
	fs.buildTable(2, len(stats))
	for i, v := range stats {
		fs.setTxt(0, i, strconv.Itoa(i))
		fs.setTxt(1, i, v.Name)
		//log.Println("Adding file", v.Name)
		if v.Directory {
			fs.setDir(1, i)
		}
		// Add in an event handler on mouse click to do whatever flProcessHandler wants to
		fs.ltab[i][1].AddEHandler(&flProcessHandler{lab: fs.ltab[i][1], prev: prev, fs: fs, win: win}, gwu.ETypeClick)
	}
}

type myWindow struct {
	gwu.Window
}

//Stats represents the status of a file
type Stats struct {
	Name      string
	Directory bool
	Exe       bool
}

func (s Stats) String() string {
	// TBD update this to return more
	return s.Name
}

type flProcessHandler struct {
	lab  gwu.Label
	prev gwu.Panel
	fs   *FlSt
	win  myWindow
}

func (h *flProcessHandler) HandleEvent(e gwu.Event) {
	if _, isLabel := e.Src().(gwu.Label); isLabel {
		path := h.fs.dir + "/" + h.lab.Text()
		// TBD add in test for if is a directory
		h.changeDir(path, e)
	}
}
func (h *flProcessHandler) changeDir(
	path string,
	e gwu.Event) {
	prev := h.prev
	win := h.win
	NP := newPanel(path, prev, win)
	log.Printf("CD to directory:%s\n%v\n", path, NP)

	h.win.Remove(prev)
	h.win.Add(NP)
	e.MarkDirty(h.win)
}

type dirProcessHandler struct {
	dir     string
	win     myWindow
	current gwu.Panel
	parent  gwu.Panel
}

func (h *dirProcessHandler) HandleEvent(e gwu.Event) {
	if _, isButton := e.Src().(gwu.Button); isButton {
		current := h.current
		prev := h.parent
		log.Println("Button Pressed with", h.win, prev)
		h.win.Remove(current)
		h.win.Add(prev)
		//e.MarkDirty(current)
		//e.MarkDirty(prev)
		e.MarkDirty(h.win)
	}
}
