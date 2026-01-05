package consumers

import (
	"encoding/json"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

// priorityKey orders by destinations (asc), size (desc), then path (asc).
// It is used for the prioritized source files treap.
type priorityKey struct {
	DestCount int
	InvSize   uint64
	Path      string
}

func (k priorityKey) Value() priorityKey { return k }
func (k priorityKey) SizeInBytes() int {
	b, _ := k.Marshal()
	return len(b)
}
func (k priorityKey) Equals(other priorityKey) bool {
	return k.DestCount == other.DestCount && k.InvSize == other.InvSize && k.Path == other.Path
}
func (k priorityKey) Marshal() ([]byte, error) {
	return json.Marshal(k)
}
func (k *priorityKey) Unmarshal(data []byte) error {
	return json.Unmarshal(data, k)
}
func (k priorityKey) New() treap.PersistentKey[priorityKey] {
	return &priorityKey{}
}

func (k priorityKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	b, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, b)
}

func (k *priorityKey) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

func priorityKeyLess(a, b priorityKey) bool {
	if a.DestCount != b.DestCount {
		return a.DestCount < b.DestCount
	}
	if a.InvSize != b.InvSize {
		return a.InvSize < b.InvSize // InvSize smaller means original size larger
	}
	// Technically we don't care about path ordering here, but why not eh?
	return a.Path < b.Path
}

// buildPriorityKey encodes ordering: fewest destinations (asc), largest size (desc), path (asc).
func buildPriorityKey(fd fileData) priorityKey {
	return priorityKey{
		DestCount: len(fd.BackupDest),
		InvSize:   ^uint64(fd.Size),
		Path:      string(fd.Fpath),
	}
}
