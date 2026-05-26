package consumers

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// restoreContentKey is the unique key for a piece of content, identified by checksum and size.
// Multiple restore targets can reference the same content via this key.
type restoreContentKey struct {
	MD5  string
	Size int64
}

func (k restoreContentKey) Value() restoreContentKey { return k }

func (k restoreContentKey) SizeInBytes() int {
	b, _ := k.Marshal()
	return len(b)
}

func (k restoreContentKey) Equals(other restoreContentKey) bool {
	return k.MD5 == other.MD5 && k.Size == other.Size
}

func (k restoreContentKey) Marshal() ([]byte, error) {
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

func (k *restoreContentKey) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("restoreContentKey data too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	if int(length) > len(data)-4 {
		return fmt.Errorf("restoreContentKey length %d exceeds data size %d", length, len(data)-4)
	}
	return json.Unmarshal(data[4:4+length], k)
}

func (k restoreContentKey) New() types.PersistentKey[restoreContentKey] {
	return &restoreContentKey{}
}

func (k restoreContentKey) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	b, err := k.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}
	id, err := store.WriteNewObjFromBytes(stre, b)
	if err != nil {
		return 0, 0, func() error { return err }
	}
	return id, len(b), func() error { return nil }
}

func (k *restoreContentKey) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error {
		return store.ReadGeneric(stre, k, id)
	}
}

func (k restoreContentKey) MarshalToObjectId(stre store.Storer) (bobbob.ObjectId, error) {
	b, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, b)
}

func (k *restoreContentKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

// DeleteDependents is a no-op for restoreContentKey since it stores its value directly in the ObjectId.
func (k restoreContentKey) DeleteDependents(stre bobbob.Storer) error {
	return nil
}

// restoreContentKeyLess orders by (MD5, Size) for efficient searching by content.
func restoreContentKeyLess(a, b restoreContentKey) bool {
	if a.MD5 != b.MD5 {
		return a.MD5 < b.MD5
	}
	return a.Size < b.Size
}

// NewRestoreContentKey constructs a content key from MD5 and Size.
func NewRestoreContentKey(md5 string, size int64) restoreContentKey {
	return restoreContentKey{
		MD5:  md5,
		Size: size,
	}
}
