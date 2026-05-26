package consumers

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// Restore DB contract constants.
const (
	RestoreDBContractVersion = "v1"

	RestoreCollectionPending = "restore_pending"
	RestoreCollectionCopied  = "restore_copied"
)

// RestoreTaskTarget represents one restore destination for a piece of content.
type RestoreTaskTarget struct {
	TaskID          string
	Alias           string
	TargetAbsPath   string
	CreatedAtUnix   int64
	UpdatedAtUnix   int64
	CompletedAtUnix int64
	CopySourcePath  string
}

// RestoreTaskNode is the payload stored in the restore DB, containing all targets for a piece of content.
// Multiple restore destinations may need the same content (MD5, Size).
type RestoreTaskNode struct {
	MD5         string
	Size        int64
	BackupDests []string           // All known backup locations that have this content
	Targets     []RestoreTaskTarget // All destinations that need this content
}

// NewRestoreTaskNode creates a payload for one restore target and content identity.
func NewRestoreTaskNode(target RestoreTaskTarget, md5 string, size int64, backupDests []string) RestoreTaskNode {
	return RestoreTaskNode{
		MD5:         md5,
		Size:        size,
		BackupDests: append([]string(nil), backupDests...),
		Targets:     []RestoreTaskTarget{target},
	}
}

// WithTarget returns a copy of the node with one more target and merged backup destinations.
func (n RestoreTaskNode) WithTarget(target RestoreTaskTarget, backupDests []string) RestoreTaskNode {
	n.Targets = append(n.Targets, target)
	n.BackupDests = mergeBackupDestinations(n.BackupDests, backupDests)
	return n
}

// RemoveTarget returns a copy of the node without the target at targetPath.
// The returned target is the removed target, and the boolean reports success.
func (n RestoreTaskNode) RemoveTarget(targetPath string) (RestoreTaskNode, RestoreTaskTarget, bool) {
	for i := range n.Targets {
		if n.Targets[i].TargetAbsPath != targetPath {
			continue
		}

		removed := n.Targets[i]
		n.Targets = append(n.Targets[:i], n.Targets[i+1:]...)
		return n, removed, true
	}

	return n, RestoreTaskTarget{}, false
}

// Marshal implements PersistentPayload interface for bobbob persistence.
func (n RestoreTaskNode) Marshal() ([]byte, error) {
	jsonData, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	// Length-prefixed format to handle fixed-size block allocations
	length := uint32(len(jsonData))
	buf := make([]byte, 4+len(jsonData))
	binary.LittleEndian.PutUint32(buf[0:4], length)
	copy(buf[4:], jsonData)
	return buf, nil
}

// Unmarshal implements PersistentPayload interface for bobbob persistence.
func (n RestoreTaskNode) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("RestoreTaskNode too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	if int(length) > len(data)-4 {
		return nil, fmt.Errorf("RestoreTaskNode length %d exceeds data size %d", length, len(data)-4)
	}
	var node RestoreTaskNode
	err := json.Unmarshal(data[4:4+length], &node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// SizeInBytes implements PersistentPayload interface.
func (n RestoreTaskNode) SizeInBytes() int {
	data, err := n.Marshal()
	if err != nil {
		return 0
	}
	return len(data)
}
