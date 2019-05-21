package hash4go

import (
	"fmt"
	"hash"
	"hash/crc32"
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
	hash       hash.Hash32
	mu         sync.RWMutex
	hashList   uint32List
	hashSum    map[uint32]string
	hashMember map[string]int
}

func NewConsistentHash(hash hash.Hash32) *ConsistentHash {
	var ch = &ConsistentHash{}
	ch.hash = hash

	if ch.hash == nil {
		ch.hash = crc32.NewIEEE()
	}

	ch.hashSum = make(map[uint32]string)
	ch.hashMember = make(map[string]int)
	return ch
}

func (this *ConsistentHash) Add(key string, node int) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if node <= 0 {
		node = 1
	}

	for i := 0; i < node; i++ {
		this.hashSum[this.doHash(key, i)] = key
	}

	this.hashMember[key] = node

	this.sort()
}

func (this *ConsistentHash) doHash(key string, node int) uint32 {
	var h = this.hash
	defer h.Reset()
	h.Write([]byte(fmt.Sprintf("%s:%d", key, node)))
	var sum = h.Sum32()
	return sum
}

func (this *ConsistentHash) Del(key string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	var replica = this.hashMember[key]
	for i := 0; i < replica; i++ {
		delete(this.hashSum, this.doHash(key, i))
	}

	delete(this.hashMember, key)
	this.sort()
}

func (this *ConsistentHash) Get(key string) string {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if len(this.hashMember) == 0 {
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

	return this.hashSum[this.hashList[index]]
}

func (this *ConsistentHash) sort() {
	hList := this.hashList[:0]

	for sum := range this.hashSum {
		hList = append(hList, sum)
	}

	hList.Sort()

	this.hashList = hList
}
