package medorg

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestVolumeCfgFromDir0(t *testing.T) {
	wkDir, err := ioutil.TempDir("", "volLabTest")
	if err != nil {
		t.Error("TmpDir Error:", err)
	}
	defer os.RemoveAll(wkDir)

	xc := XMLCfg{}
	label, err := getVolumeLabel(&xc, wkDir)
	if err != nil {
		t.Error("Error:", err)
	}
	if label == "" {
		t.Error("Empty Label")
	}
	t.Log("Got Label:", label)
}
func TestVolumeCfgFromDir1(t *testing.T) {
	wkDir, err := ioutil.TempDir("", "volLabTest")
	if err != nil {
		t.Error("TmpDir Error:", err)
	}
	defer os.RemoveAll(wkDir)

	xc := XMLCfg{}
	vc, err := VolumeCfgFromDir(&xc, wkDir)
	if err != nil {
		t.Error("Error vcd:", err)
	}
	vc.Persist()
	label0 := vc.Label
	t.Log("Got Label0:", label0)

	newDir := filepath.Join(wkDir, RandStringBytesMaskImprSrcSB(4))
	createDestDirectoryAsNeeded(filepath.Join(newDir, RandStringBytesMaskImprSrcSB(6)))
	xc1 := XMLCfg{}

	label1, err := getVolumeLabel(&xc1, newDir)
	if err != nil {
		t.Error("Error1:", err)
	}
	if label1 == "" {
		t.Error("Empty Label1")
	}
	t.Log("Got Label1:", label1)

	if label0 != label1 {
		t.Error("Bang:", label0, label1)
	}
}
