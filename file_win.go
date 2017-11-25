package medorg

import "github.com/icza/gowut/gwu"

// FlSt File Structure
type FlSt struct {
	gtab gwu.Table
	ltab [][]gwu.Label
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

// FileWin creates the window object that
// all the anagram if resides in
func FileWin(path string) gwu.Window {

	win := gwu.NewWindow("file", "File")
	win.Style().SetFullWidth()
	win.SetHAlign(gwu.HACenter)
	win.SetCellPadding(2)

	// A panel for each major thing
	panelTb := gwu.NewHorizontalPanel()
	listTable := gwu.NewTable()
	flst := FlSt{gtab: listTable}
	flst.newOutputCel(0, 0)
	flst.newOutputCel(1, 0)
	flst.setTxt(0, 0, "first!")
	flst.setTxt(1, 0, "second!")
	panelTb.Add(listTable)
	win.Add(panelTb)
	return win
}
