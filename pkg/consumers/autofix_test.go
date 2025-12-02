package consumers

import (
	"log"
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

func TestRename0(t *testing.T) {
	testMode = true
	DomainList := []string{
		"(.*)_calc",
	}

	AF := NewAutoFix(DomainList)
	fs := core.FileStruct{Name: "test_calc.flv"}
	fs, mod := AF.CheckRename(fs)
	if mod {
		t.Fatal("Modified while disabled", fs)
	} else {
		if fs.Name != "test_calc.flv" {
			t.Fatal("Name was modified", fs)
		}
	}
	AF.RenameFiles = true
	fs, mod = AF.CheckRename(fs)
	if mod {
		log.Println("FS is now", fs)
	} else {
		t.Fatal("Not modified", fs)
	}
}

type renameStruct struct {
	In     string
	Out    string
	Modify bool
}

func TestRename1(t *testing.T) {
	testMode = true
	DomainList := []string{
		"(.*)_calc",
		"(.*)_bob_(.*)",
	}
	testStruct := []renameStruct{
		{"test_calc.flv", "test.flv", true},
		{"test_calc.flv.flv", "test.flv", true},
		{"test_calc.mp4.flv", "test.flv", true},
		{"test_calc", "test_calc", false},
		{"test_bob_c.mpg", "testc.mpg", true},
		{"test_calc_bob.jpg", "test.jpg", true},
		{"Party.mp4.mp4", "Party.mp4", true},
		{"This is a - weird filename.wmv.mp4", "This is a - weird filename.mp4", true},
		{"fred.jpg.doc", "fred.jpg.doc", false},
		{"/wibble.com_4cbb7934338409b928a4ee6b86725738.mp4.mp4", "/wibble.com_4cbb7934338409b928a4ee6b86725738.mp4", true},
	}
	AF := NewAutoFix(DomainList)
	AF.RenameFiles = true
	var mod bool
	var fs core.FileStruct
	for _, ts := range testStruct {
		fn0 := ts.In
		fn1 := ts.Out
		// Create a synthetic FileStruct for testing (testMode = true means files don't exist on disk)
		fs = core.FileStruct{Name: fn0}
		fs.SetDirectory(".")

		fs, mod = AF.CheckRename(fs)
		if mod == ts.Modify {
			if fs.Name == fn1 {
				log.Println("FS is now", fn0, fn1)
			} else {
				t.Fatal("Incorrectly modified:", fn0, fn1, fs.Name)
			}
		} else {
			t.Fatal("Not modified", fn0, fn1, fs.Name)
		}
	}
}
