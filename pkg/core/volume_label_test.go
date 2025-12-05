package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVolumeCfgFromDir0(t *testing.T) {
	wkDir, err := os.MkdirTemp("", "volLabTest")
	if err != nil {
		t.Error("TmpDir Error:", err)
	}
	defer os.RemoveAll(wkDir)

	xc := MdConfig{}
	label, err := xc.GetVolumeLabel(wkDir)
	if err != nil {
		t.Error("Error:", err)
	}
	if label == "" {
		t.Error("Empty Label")
	}
	t.Log("Got Label:", label)
}

func TestVolumeCfgFromDir1(t *testing.T) {
	wkDir, err := os.MkdirTemp("", "volLabTest")
	if err != nil {
		t.Error("TmpDir Error:", err)
	}
	defer os.RemoveAll(wkDir)

	xc := MdConfig{}
	vc, err := xc.VolumeCfgFromDir(wkDir)
	if err != nil {
		t.Error("Error vcd:", err)
	}
	if err := vc.Persist(); err != nil {
		t.Fatal(err)
	}
	label0 := vc.Label
	t.Log("Got Label0:", label0)

	newDir := filepath.Join(wkDir, RandStringBytesMaskImprSrcSB(4))
	if err := createDestDirectoryAsNeeded(filepath.Join(newDir, RandStringBytesMaskImprSrcSB(6))); err != nil {
		t.Fatal(err)
	}
	xc1 := MdConfig{}

	label1, err := xc1.GetVolumeLabel(newDir)
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
