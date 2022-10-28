package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

// 一致性hash算法

type HashFunc func(data []byte) uint32

type Map struct {
	hashFunc HashFunc  // hash函数
	replicas int       // 每个物理节点会产生 replicas个虚拟节点
	keys []int         // 虚拟节点的集合 - hash环 (有序的)
	hashMap map[int]string   // 虚拟节点 hash值 到物理节点地址的映射关系
}

func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		hashFunc: fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		// 默认的hash函数
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}

		for i := 0; i < m.replicas; i++ {
			// 使用"key+i"作为一个虚拟节点，计算出对应的hash值
			hash := int(m.hashFunc([]byte(key + strconv.Itoa(i))))
			// 将虚拟节点添加到hash环上
			m.keys = append(m.keys, hash)
			// 建立虚拟节点hash值与物理节点的映射关系
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// 支持hash tag
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg + 1 {
		return key
	}
	return key[beg+1:end]
}

func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}
	if key == "" {
		return ""
	}

	// 支持根据 key 的 hashtag 来确定分布
	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))

	// 二分查找，顺着hash环找到与key的hash值接近的第一个虚拟节点的下标
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if index == len(m.keys) {
		index = 0
	}
	// 找到虚拟节点的hash值
	targetHash := m.keys[index]

	// 返回物理节点的地址
	return m.hashMap[targetHash]
}

