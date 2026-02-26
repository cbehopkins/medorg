package main

import (
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator"
	"github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/medorg/pkg/cli"
)

func main() {
	cli.ExitFromRun(run())
}

type config struct {
	vaultPath string
	objID     string
	outFormat string
	outFile   string
	limit     int
}

func run() (int, error) {
	cfg := parseFlags()
	if cfg.vaultPath == "" || cfg.objID == "" {
		printUsage()
		return cli.ExitInvalidArgs, nil
	}

	objID, err := parseObjectID(cfg.objID)
	if err != nil {
		return cli.ExitInvalidArgs, err
	}

	s, err := multistore.LoadConcurrentMultiStore(cfg.vaultPath, 0)
	if err != nil {
		return cli.ExitConfigError, fmt.Errorf("failed to load store %q: %w", cfg.vaultPath, err)
	}
	defer func() {
		if cerr := s.Close(); cerr != nil {
			log.Printf("warning: failed to close store: %v", cerr)
		}
	}()

	var info store.ObjectInfo
	var infoFound bool
	if infoProvider, ok := s.(store.ObjectInfoProvider); ok {
		if i, ok := infoProvider.GetObjectInfo(store.ObjectId(objID)); ok {
			info = i
			infoFound = true
			log.Printf("ObjectId %d: offset=%d size=%d", objID, info.Offset, info.Size)
		}
	}
	if !infoFound {
		log.Printf("ObjectId %d not found in allocator", objID)
		logAllocatorSummary(s, store.ObjectId(objID))
	}

	if st, statErr := os.Stat(cfg.vaultPath); statErr == nil {
		log.Printf("Vault file size: %d bytes", st.Size())
		if infoFound {
			offEnd := int64(info.Offset) + int64(info.Size)
			if offEnd > st.Size() {
				log.Printf("WARNING: object end offset %d exceeds file size %d", offEnd, st.Size())
			}
		}
	}

	data, err := store.ReadBytesFromObj(s, store.ObjectId(objID))
	if err != nil {
		if infoFound {
			log.Printf("ReadBytesFromObj failed: %v", err)
			if raw, rawErr := readRawObjectBytes(cfg.vaultPath, info); rawErr == nil {
				log.Printf("Raw read succeeded (%d bytes)", len(raw))
				data = raw
			} else {
				return cli.ExitMetadataError, fmt.Errorf("failed to read object %d: %w (raw read error: %v)", objID, err, rawErr)
			}
		} else {
			return cli.ExitMetadataError, fmt.Errorf("failed to read object %d: %w", objID, err)
		}
	}

	if cfg.outFile != "" {
		if err := os.WriteFile(cfg.outFile, data, 0o644); err != nil {
			return cli.ExitMetadataError, fmt.Errorf("failed to write %q: %w", cfg.outFile, err)
		}
		log.Printf("Wrote %d bytes to %s", len(data), cfg.outFile)
	}

	dataToPrint, truncated := applyLimit(data, cfg.limit)
	if truncated {
		log.Printf("Output truncated to %d bytes (total=%d)", len(dataToPrint), len(data))
	} else {
		log.Printf("Output size=%d bytes", len(dataToPrint))
	}

	switch strings.ToLower(cfg.outFormat) {
	case "hex":
		fmt.Print(hex.Dump(dataToPrint))
	case "base64":
		fmt.Println(base64.StdEncoding.EncodeToString(dataToPrint))
	case "raw":
		_, _ = os.Stdout.Write(dataToPrint)
	default:
		return cli.ExitInvalidArgs, fmt.Errorf("unknown output format: %s", cfg.outFormat)
	}

	return cli.ExitOk, nil
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.vaultPath, "vault", "", "Path to vault file (db)")
	flag.StringVar(&cfg.objID, "id", "", "ObjectId (decimal or 0x... hex)")
	flag.StringVar(&cfg.outFormat, "out", "hex", "Output format: hex|base64|raw")
	flag.StringVar(&cfg.outFile, "out-file", "", "Write raw bytes to file")
	flag.IntVar(&cfg.limit, "limit", 512, "Max bytes to print (0 = no limit)")
	flag.Parse()
	return cfg
}

func parseObjectID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid object id %q: %w", s, err)
	}
	return id, nil
}

func applyLimit(data []byte, limit int) ([]byte, bool) {
	if limit <= 0 || len(data) <= limit {
		return data, false
	}
	return data[:limit], true
}

func readRawObjectBytes(path string, info store.ObjectInfo) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	if info.Size <= 0 {
		return nil, fmt.Errorf("invalid object size %d", info.Size)
	}

	buf := make([]byte, info.Size)
	n, err := f.ReadAt(buf, int64(info.Offset))
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

func logAllocatorSummary(s store.Storer, targetID store.ObjectId) {
	log.Printf("Store type: %T", s)
	provider, ok := s.(store.AllocatorProvider)
	if !ok {
		return
	}
	alloc := provider.Allocator()
	if alloc == nil {
		return
	}
	log.Printf("Allocator type: %T", alloc)
	if contains, ok := alloc.(interface{ ContainsObjectId(bobbob.ObjectId) bool }); ok {
		log.Printf("Allocator ContainsObjectId(%d) = %v", targetID, contains.ContainsObjectId(bobbob.ObjectId(targetID)))
	}

	if top, ok := alloc.(*allocator.Top); ok {
		if omni := getOmniAllocator(top); omni != nil {
			logOmniRanges(omni)
			return
		}
	}
	introspect, ok := alloc.(interface {
		GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []types.ObjectId
	})
	if !ok {
		return
	}

	blockSizes := []int{64, 256, 1024}
	for _, blockSize := range blockSizes {
		emptyStreak := 0
		maxIndex := 4096
		for idx := 0; idx < maxIndex; idx++ {
			ids := introspect.GetObjectIdsInAllocator(blockSize, idx)
			if len(ids) == 0 {
				emptyStreak++
				if emptyStreak >= 128 {
					break
				}
				continue
			}
			emptyStreak = 0
			minID, maxID := minMaxObjectIDs(ids)
			log.Printf("Allocator blockSize=%d index=%d count=%d min=%d max=%d", blockSize, idx, len(ids), minID, maxID)
		}
	}
}

func getOmniAllocator(top *allocator.Top) any {
	if top == nil {
		return nil
	}
	v := reflect.ValueOf(top).Elem().FieldByName("omniAllocator")
	if !v.IsValid() || v.IsNil() {
		return nil
	}
	ptr := reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
	return ptr.Interface()
}

func logOmniRanges(omni any) {
	introspect, ok := omni.(interface {
		GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []types.ObjectId
	})
	if !ok {
		return
	}
	blockSizes := []int{64, 256, 1024}
	for _, blockSize := range blockSizes {
		ids := introspect.GetObjectIdsInAllocator(blockSize, 0)
		if len(ids) == 0 {
			continue
		}
		minID, maxID := minMaxObjectIDs(ids)
		log.Printf("OmniAllocator blockSize=%d count=%d min=%d max=%d", blockSize, len(ids), minID, maxID)
	}
}

func minMaxObjectIDs(ids []types.ObjectId) (types.ObjectId, types.ObjectId) {
	minID := ids[0]
	maxID := ids[0]
	for _, id := range ids[1:] {
		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
	}
	return minID, maxID
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: mddumpobj -vault <path> -id <objectId> [-out hex|base64|raw] [-out-file path] [-limit N]")
	fmt.Fprintln(os.Stderr, "Example: mddumpobj -vault /tmp/backup.db -id 6755110 -out hex -limit 256")
}
