package consumers

import (
	"sort"

	"github.com/cbehopkins/medorg/pkg/core"
)

func prioritizeFiles(candidates []core.FileStruct, label string) []core.FileStruct {
	filterFunc := func(can core.FileStruct) bool {
		// If the file is already backed up at the provided label
		for _, target := range can.BackupDest {
			if target == label {
				return false
			}
		}
		return true
	}

	newCands := make([]core.FileStruct, 0, len(candidates))
	if label != "" {
		for _, v := range candidates {
			if filterFunc(v) {
				newCands = append(newCands, v)
			}
		}
	} else {
		newCands = append(newCands, candidates...)
	}
	sort.Slice(newCands, func(i, j int) bool {
		return newCands[i].Size > newCands[j].Size
	})
	sort.Slice(newCands, func(i, j int) bool {
		return len(newCands[i].BackupDest) < len(newCands[j].BackupDest)
	})
	return newCands
}
