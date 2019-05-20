package hash4go

import (
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"sort"
	"sync"
)

// --------------------------------------------------------------------------------
type uint32List []uint32

func (n uint32List) Len() int {
	return len(n)
}

func (n uint32List) Less(i, j int) bool {
	return n[i] < n[j]
}

func (n uint32List) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n uint32List) Sort() {
	sort.Sort(n)
}

// --------------------------------------------------------------------------------
type ConsistentHash struct {
	replicas int
	hash     hash.Hash32
	mu       sync.RWMutex
	hashList uint32List
	hashKeys map[uint32]string
	weights  map[string]int // 用于存储节点的权重信息
}

func NewConsistentHash(replicas int, hash hash.Hash32) *ConsistentHash {
	var ch = &ConsistentHash{}
	ch.replicas = replicas
	ch.hash = hash

	if ch.hash == nil {
		ch.hash = crc32.NewIEEE()
	}

	ch.hashKeys = make(map[uint32]string)
	ch.weights = make(map[string]int)
	return ch
}

func (this *ConsistentHash) Add(key string, weight int) {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.weights[key] = weight
	this.update()
}

func (this *ConsistentHash) Del(key string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	delete(this.weights, key)
	this.update()
}

func (this *ConsistentHash) Get(key string) string {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if len(this.weights) == 0 {
		return ""
	}

	var h = this.hash
	defer h.Reset()

	if _, err := h.Write([]byte(key)); err != nil {
		return ""
	}

	var sum = h.Sum32()

	var index = sort.Search(len(this.hashList), func(i int) bool {
		return this.hashList[i] >= sum
	})

	if index == len(this.hashList) {
		index = 0
	}

	return this.hashKeys[this.hashList[index]]
}

func (this *ConsistentHash) update() {
	var totalWeight int
	for _, w := range this.weights {
		totalWeight += w
	}
	var totalNode = this.replicas * len(this.weights)

	this.hashList = this.hashList[:0]
	this.hashKeys = make(map[uint32]string)

	var h = this.hash

	for key, weight := range this.weights {
		var node = int(math.Floor(float64(weight) / float64(totalWeight) * float64(totalNode)))
		for i := 0; i < node; i++ {
			h.Write([]byte(fmt.Sprintf("%s:%d", key, i)))
			var sum = h.Sum32()
			this.hashList = append(this.hashList, sum)
			this.hashKeys[sum] = key
			h.Reset()
		}
	}
	this.hashList.Sort()
}
