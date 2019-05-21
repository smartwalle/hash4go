package hash4go

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"sort"
	"sync"
)

var (
	ErrNotFoundElement = errors.New("hash4go: not found element")
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
	mu          sync.RWMutex
	hash        hash.Hash32
	hashSumList uint32List        // 用于存放排序之后 hash 值
	hashSum     map[uint32]string // 用于存放 hash 值及其对应的 key
	hashMember  map[string]int    // 用于存放 key 及其对应的节点数量
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
		this.hashSum[this.getHashSum(fmt.Sprintf("%s:%d", key, i))] = key
	}

	this.hashMember[key] = node

	this.sort()
}

func (this *ConsistentHash) Del(key string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	var node = this.hashMember[key]
	for i := 0; i < node; i++ {
		delete(this.hashSum, this.getHashSum(fmt.Sprintf("%s:%d", key, i)))
	}

	delete(this.hashMember, key)
	this.sort()
}

func (this *ConsistentHash) Get(key string) (string, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if len(this.hashMember) == 0 || len(this.hashSumList) == 0 {
		return "", ErrNotFoundElement
	}

	var sum = this.getHashSum(key)

	var index = sort.Search(len(this.hashSumList), func(i int) bool {
		return this.hashSumList[i] >= sum
	})

	if index >= len(this.hashSumList) {
		index = 0
	}

	return this.hashSum[this.hashSumList[index]], nil
}

func (this *ConsistentHash) getHashSum(key string) uint32 {
	var h = this.hash
	defer h.Reset()
	h.Write([]byte(key))
	var sum = h.Sum32()
	return sum
}

func (this *ConsistentHash) sort() {
	hList := this.hashSumList[:0]

	for sum := range this.hashSum {
		hList = append(hList, sum)
	}

	hList.Sort()

	this.hashSumList = hList
}
