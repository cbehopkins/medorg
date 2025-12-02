package consumers

import (
	"testing"

	"github.com/cbehopkins/medorg/pkg/core"
)

// Helper to create test FileStructs since directory field is unexported
func makeTestFileStruct(dir, name, checksum string, size int64, backupDest []string) core.FileStruct {
	fs := core.FileStruct{
		Name:       name,
		Checksum:   checksum,
		Size:       size,
		BackupDest: backupDest,
	}
	fs.SetDirectory(dir)
	return fs
}

type tc struct {
	description   string
	fss           []core.FileStruct
	expectedOrder []int
}

func TestPrioritize(t *testing.T) {
	testcases := []tc{
		{
			description: "Test that when reversed, larger files are first",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "some_file.mp3", "abcdef", 123, nil),
				makeTestFileStruct("here", "another_file.mp3", "ghijk", 321, nil),
			},
			expectedOrder: []int{1, 0},
		},
		{
			description: "Test that larger files are still first",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "some_file.mp3", "abcdef", 321, nil),
				makeTestFileStruct("here", "another_file.mp3", "ghijk", 123, nil),
			},
			expectedOrder: []int{0, 1},
		},
		{
			description: "Test that files of same size don't confuse things",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "some_file.mp3", "abcdef", 123, nil),
				makeTestFileStruct("here", "some_file.mp3", "ghijk", 123, nil),
			},
			expectedOrder: []int{0, 1},
		},
		{
			description: "Test with many files",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "a.mp3", "abcdef", 6, nil),
				makeTestFileStruct("here", "b.mp3", "ghijk", 5, nil),
				makeTestFileStruct("here", "c.mp3", "ghijk", 4, nil),
				makeTestFileStruct("here", "d.mp3", "ghijk", 3, nil),
				makeTestFileStruct("here", "e.mp3", "ghijk", 2, nil),
			},
			expectedOrder: []int{0, 1, 2, 3, 4},
		},
		{
			description: "Test with many files shuffled",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "a.mp3", "abcdef", 4, nil),
				makeTestFileStruct("here", "b.mp3", "ghijk", 2, nil),
				makeTestFileStruct("here", "c.mp3", "ghijk", 6, nil),
				makeTestFileStruct("here", "d.mp3", "ghijk", 3, nil),
				makeTestFileStruct("here", "e.mp3", "ghijk", 5, nil),
			},
			expectedOrder: []int{2, 4, 0, 3, 1},
		},
		{
			description: "ArchivedAt Length ordering",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "some_file.mp3", "abcdef", 123, nil),
				makeTestFileStruct("here", "another_file.mp3", "ghijk", 123, []string{"bob"}),
			},
			expectedOrder: []int{0, 1},
		},
		{
			description: "ArchivedAt Length ordering reverse",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "some_file.mp3", "abcdef", 123, []string{"bob"}),
				makeTestFileStruct("here", "another_file.mp3", "ghijk", 123, nil),
			},
			expectedOrder: []int{1, 0},
		},
		{
			description: "ArchivedAt Length ordering has priority",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "small_file_not_backed.mp3", "abcdef", 123, nil),
				makeTestFileStruct("here", "larger_file_not_backed.mp3", "abcdef", 321, nil),
				makeTestFileStruct("here", "small_backed.mp3", "ghijk", 123, []string{"bob"}),
				makeTestFileStruct("here", "larger_backed.mp3", "ghijk", 321, []string{"bob"}),
			},
			expectedOrder: []int{1, 0, 3, 2},
		},
		{
			description: "ArchivedAt Length ordering has priority shuffled",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "small_backed.mp3", "ghijk", 123, []string{"bob"}),
				makeTestFileStruct("here", "small_file_not_backed.mp3", "abcdef", 123, nil),
				makeTestFileStruct("here", "larger_file_not_backed.mp3", "abcdef", 321, nil),
				makeTestFileStruct("here", "larger_backed.mp3", "ghijk", 321, []string{"bob"}),
			},
			expectedOrder: []int{2, 1, 3, 0},
		},
		{
			description: "ArchivedAt label excluded",
			fss: []core.FileStruct{
				makeTestFileStruct("here", "small_backed.mp3", "ghijk", 123, []string{"bob"}),
				makeTestFileStruct("here", "small_file_not_backed.mp3", "abcdef", 123, nil),
				makeTestFileStruct("here", "larger_file_not_backed.mp3", "abcdef", 321, nil),
				makeTestFileStruct("here", "missing.mp3", "ghijk", 512, []string{"labby"}),
				makeTestFileStruct("here", "missing_again.mp3", "ghijk", 512, []string{"bob", "labby"}),
				makeTestFileStruct("here", "larger_backed.mp3", "ghijk", 321, []string{"bob"}),
			},
			expectedOrder: []int{2, 1, 5, 0},
		},
	}
	for n, testcase := range testcases {
		t.Run(testcase.description, func(t *testing.T) {
			expectedOrder := testcase.expectedOrder
			prioritizedOrder := prioritizeFiles(testcase.fss, "labby")
			t.Log(prioritizedOrder, "\n\n")
			if len(expectedOrder) != len(prioritizedOrder) {
				t.Error("Testcase:", n, ":", testcase.description, "Lengths don't match", len(expectedOrder), len(prioritizedOrder))
			}
			for i := range expectedOrder {
				if !prioritizedOrder[i].Equal(testcase.fss[expectedOrder[i]]) {
					t.Error("Testcase:", n, ":", testcase.description, "Order", prioritizedOrder[i], "didn't match", testcase.fss[expectedOrder[i]])
				}
			}
		})
	}
}
