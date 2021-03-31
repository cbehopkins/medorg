package medorg

import (
	"testing"
)

type tc struct {
	description   string
	fss           []FileStruct
	expectedOrder []int
}

func TestPrioritize(t *testing.T) {
	testcases := []tc{
		tc{
			description: "Test that when reversed, larger files are first",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "some_file.mp3",
					Checksum:  "abcdef",
					Size:      123,
				}, {
					directory: "here",
					Name:      "another_file.mp3",
					Checksum:  "ghijk",
					Size:      321,
				}},
			expectedOrder: []int{1, 0}},
		tc{
			description: "Test that larger files are still first",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "some_file.mp3",
					Checksum:  "abcdef",
					Size:      321,
				}, {
					directory: "here",
					Name:      "another_file.mp3",
					Checksum:  "ghijk",
					Size:      123,
				}},
			expectedOrder: []int{0, 1}},
		tc{
			description: "Test that files of same size don't confuse things",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "some_file.mp3",
					Checksum:  "abcdef",
					Size:      123,
				}, {
					directory: "here",
					Name:      "some_file.mp3",
					Checksum:  "ghijk",
					Size:      123,
				}},
			expectedOrder: []int{0, 1}},
		tc{
			description: "Test with many files",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "a.mp3",
					Checksum:  "abcdef",
					Size:      6,
				}, {
					directory: "here",
					Name:      "b.mp3",
					Checksum:  "ghijk",
					Size:      5,
				}, {
					directory: "here",
					Name:      "c.mp3",
					Checksum:  "ghijk",
					Size:      4,
				}, {
					directory: "here",
					Name:      "d.mp3",
					Checksum:  "ghijk",
					Size:      3,
				}, {
					directory: "here",
					Name:      "e.mp3",
					Checksum:  "ghijk",
					Size:      2,
				}},
			expectedOrder: []int{0, 1, 2, 3, 4}},
		tc{
			description: "Test with many files shuffled",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "a.mp3",
					Checksum:  "abcdef",
					Size:      4,
				}, {
					directory: "here",
					Name:      "b.mp3",
					Checksum:  "ghijk",
					Size:      2,
				}, {
					directory: "here",
					Name:      "c.mp3",
					Checksum:  "ghijk",
					Size:      6,
				}, {
					directory: "here",
					Name:      "d.mp3",
					Checksum:  "ghijk",
					Size:      3,
				}, {
					directory: "here",
					Name:      "e.mp3",
					Checksum:  "ghijk",
					Size:      5,
				}},
			expectedOrder: []int{2, 4, 0, 3, 1}},
		tc{
			description: "ArchivedAt Length ordering",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "some_file.mp3",
					Checksum:  "abcdef",
					Size:      123,
				}, {
					directory:  "here",
					Name:       "another_file.mp3",
					Checksum:   "ghijk",
					Size:       123,
					ArchivedAt: []string{"bob"},
				}},
			expectedOrder: []int{0, 1}},
		tc{
			description: "ArchivedAt Length ordering reverse",
			fss: []FileStruct{
				{
					directory:  "here",
					Name:       "some_file.mp3",
					Checksum:   "abcdef",
					Size:       123,
					ArchivedAt: []string{"bob"},
				}, {
					directory: "here",
					Name:      "another_file.mp3",
					Checksum:  "ghijk",
					Size:      123,
				}},
			expectedOrder: []int{1, 0}},
		tc{
			description: "ArchivedAt Length ordering has priority",
			fss: []FileStruct{
				{
					directory: "here",
					Name:      "small_file_not_backed.mp3",
					Checksum:  "abcdef",
					Size:      123,
				}, {
					directory: "here",
					Name:      "larger_file_not_backed.mp3",
					Checksum:  "abcdef",
					Size:      321,
				}, {
					directory:  "here",
					Name:       "small_backed.mp3",
					Checksum:   "ghijk",
					Size:       123,
					ArchivedAt: []string{"bob"},
				}, {
					directory:  "here",
					Name:       "larger_backed.mp3",
					Checksum:   "ghijk",
					Size:       321,
					ArchivedAt: []string{"bob"},
				}},
			expectedOrder: []int{1, 0, 3, 2}},
		tc{
			description: "ArchivedAt Length ordering has priority shuffled",
			fss: []FileStruct{
				{
					directory:  "here",
					Name:       "small_backed.mp3",
					Checksum:   "ghijk",
					Size:       123,
					ArchivedAt: []string{"bob"},
				}, {
					directory: "here",
					Name:      "small_file_not_backed.mp3",
					Checksum:  "abcdef",
					Size:      123,
				}, {
					directory: "here",
					Name:      "larger_file_not_backed.mp3",
					Checksum:  "abcdef",
					Size:      321,
				}, {
					directory:  "here",
					Name:       "larger_backed.mp3",
					Checksum:   "ghijk",
					Size:       321,
					ArchivedAt: []string{"bob"},
				}},
			expectedOrder: []int{2, 1, 3, 0}},
		tc{
			description: "ArchivedAt label excluded",
			fss: []FileStruct{
				{
					directory:  "here",
					Name:       "small_backed.mp3",
					Checksum:   "ghijk",
					Size:       123,
					ArchivedAt: []string{"bob"},
				}, {
					directory: "here",
					Name:      "small_file_not_backed.mp3",
					Checksum:  "abcdef",
					Size:      123,
				}, {
					directory: "here",
					Name:      "larger_file_not_backed.mp3",
					Checksum:  "abcdef",
					Size:      321,
				}, {
					directory:  "here",
					Name:       "missing.mp3",
					Checksum:   "ghijk",
					Size:       512,
					ArchivedAt: []string{"labby"},
				}, {
					directory:  "here",
					Name:       "missing_again.mp3",
					Checksum:   "ghijk",
					Size:       512,
					ArchivedAt: []string{"bob", "labby"},
				}, {
					directory:  "here",
					Name:       "larger_backed.mp3",
					Checksum:   "ghijk",
					Size:       321,
					ArchivedAt: []string{"bob"},
				}},
			expectedOrder: []int{2, 1, 5, 0}},
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
