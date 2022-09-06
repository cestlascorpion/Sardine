package utils

import (
	"context"
	"hash/fnv"
	"sort"
	"strconv"
)

type Hash struct {
	circle map[uint32]string
	sorted []uint32
}

func NewHash(ctx context.Context, nodes []string) *Hash {
	circle := make(map[uint32]string, 0)
	sorted := make([]uint32, 0)
	for i := range nodes {
		for idx := 0; idx < replica; idx++ {
			hash := hashKey(eltKey(nodes[i], idx))
			circle[hash] = nodes[i]
			sorted = append(sorted, hash)
		}
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	return &Hash{
		circle: circle,
		sorted: sorted,
	}
}

func (h *Hash) Get(ctx context.Context, name string) string {
	key := hashKey(name)
	f := func(x int) bool {
		return h.sorted[x] > key
	}
	idx := sort.Search(len(h.sorted), f)
	if idx >= len(h.sorted) {
		idx = 0
	}
	return h.circle[h.sorted[idx]]
}

const replica = 512

func eltKey(elt string, idx int) string {
	return strconv.Itoa(idx) + "_" + elt
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
