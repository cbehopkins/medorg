package consumers

import (
	"bytes"
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

type md5Key [16]byte

func (k md5Key) Value() md5Key            { return k }
func (k md5Key) SizeInBytes() int         { return len(k) }
func (k md5Key) Equals(other md5Key) bool { return k == other }

func (k md5Key) Marshal() ([]byte, error) {
	b := make([]byte, len(k))
	copy(b, k[:])
	return b, nil
}

func (k *md5Key) Unmarshal(data []byte) error {
	if len(data) < len(k) {
		return fmt.Errorf("md5Key data too short: %d bytes", len(data))
	}
	copy(k[:], data[:len(k)])
	return nil
}

func (k md5Key) New() types.PersistentKey[md5Key] { return &md5Key{} }

func (k md5Key) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	id, err := store.WriteNewObjFromBytes(stre, k[:])
	if err != nil {
		return 0, 0, func() error { return err }
	}
	return id, len(k), func() error { return nil }
}

func (k *md5Key) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error {
		return store.ReadGeneric(stre, k, id)
	}
}

func (k md5Key) MarshalToObjectId(stre store.Storer) (bobbob.ObjectId, error) {
	return store.WriteNewObjFromBytes(stre, k[:])
}

func (k *md5Key) UnmarshalFromObjectId(id bobbob.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

// DeleteDependents is a no-op for md5Key since it stores its value directly in the ObjectId.
func (k md5Key) DeleteDependents(stre bobbob.Storer) error {
	return nil
}

func md5KeyLess(a, b md5Key) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

func md5KeyFromString(s string) (md5Key, string, error) {
	key, err := types.MD5KeyFromString(s)
	format := "hex"
	if err != nil {
		key, err = types.Md5KeyFromBase64String(s)
		format = "base64"
		if err != nil {
			return md5Key{}, format, err
		}
	}
	return md5Key(key), format, nil
}
