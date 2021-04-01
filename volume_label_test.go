package medorg

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestVolumeCfgFromDir(t *testing.T) {
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
