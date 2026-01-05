package consumers

import "sort"

// priorityBuckets groups fileData by a priority key (e.g., number of destinations)
// and provides ordered iteration from smallest key to largest without exposing the
// internal storage layout.
type priorityBuckets struct {
	buckets map[int][]fileData
}

func newPriorityBuckets() *priorityBuckets {
	return &priorityBuckets{buckets: make(map[int][]fileData)}
}

func (pb *priorityBuckets) add(key int, fd fileData) error {
	pb.buckets[key] = append(pb.buckets[key], fd)
	return nil
}

// iterate returns a next function that yields buckets in ascending key order.
func (pb *priorityBuckets) iterate() func() ([]fileData, bool) {
	keys := make([]int, 0, len(pb.buckets))
	for k := range pb.buckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	idx := 0

	return func() ([]fileData, bool) {
		for idx < len(keys) {
			k := keys[idx]
			idx++
			bucket := pb.buckets[k]
			if len(bucket) == 0 {
				continue
			}
			sort.Slice(bucket, func(i, j int) bool {
				if bucket[i].Size == bucket[j].Size {
					return bucket[i].Fpath < bucket[j].Fpath
				}
				return bucket[i].Size > bucket[j].Size
			})
			return bucket, true
		}
		return nil, false
	}
}
