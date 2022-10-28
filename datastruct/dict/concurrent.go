package dict

import (
	"math"
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table      []*shard
	count      int32 // key的总数
	shardCount int   // shard的数量
}

type shard struct {
	m  map[string]interface{}
	mu sync.RWMutex
}

func computeCapacity(param int) int {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	return &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
}

const prime32 = uint32(16777619)

// 哈希算法选择FNV算法
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}

	tableSize := uint32(len(dict.table))
	// 定位shard, 当n为2的整数幂时 h % n == (n - 1) & h
	// 本来应该是  hashCode / tableSize
	// 但逻辑运算比除法运算要快，所以这里使用 (tableSize - 1) & hashCode
	return (tableSize - 1) & hashCode
}

func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}

	return dict.table[index]
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	val, exists = shard.m[key]
	return
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}

	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 已存在
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	}

	// 新的key-val
	shard.m[key] = val
	dict.addCount()
	return 1
}

// PutIfAbsent key不存在则添加
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 已存在
	if _, ok := shard.m[key]; ok {
		return 0
	}

	// 新的key-val
	shard.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists key存在则更新
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 已存在
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 存在则移除指定key
	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// ForEach 遍历Dict
// it may not visits new entry inserted during traversal
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, shard := range dict.table {
		shard.mu.RLock()
		func() {
			defer shard.mu.RUnlock()
			for key, val := range shard.m {
				continues := consumer(key, val)
				if !continues {
					return
				}
			}
		}()
	}
}

// Keys 返回所有的key
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, 0, dict.Len())
	dict.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Clear 将Dict中所有key移除
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

// RandomKey 从当前shard中随机返回一个key
func (s *shard) RandomKey() string {
	if s == nil {
		panic("shard is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key := range s.m {
		return key
	}
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	return nil
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	return nil
}
