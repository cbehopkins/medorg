package consumers

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
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
	data, err := json.Marshal(k)
	if err != nil {
		return nil, err
	}
	// Length-prefixed format to handle fixed-size block allocations
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], length)
	copy(buf[4:], data)
	return buf, nil
}
func (k *priorityKey) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("priorityKey data too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	if int(length) > len(data)-4 {
		return fmt.Errorf("priorityKey length %d exceeds data size %d", length, len(data)-4)
	}
	return json.Unmarshal(data[4:4+length], k)
}
func (k priorityKey) New() types.PersistentKey[priorityKey] {
	return &priorityKey{}
}

func (k priorityKey) MarshalToObjectId(stre store.Storer) (bobbob.ObjectId, error) {
	b, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, b)
}

func (k *priorityKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre store.Storer) error {
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
		Path:      fd.Fpath.String(),
	}
}
