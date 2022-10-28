package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	// epoch0 is set to the twitter snowflake epoch of Nov 04 2010 01:42:54 UTC in milliseconds
	// You may customize this to set a different epoch for your application.
	epoch0 int64 = 1655881750657
	timeLeft uint8 = 22
	nodeLeft uint8 = 10
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	nodeMask int64 = -1 ^ (-1 << uint64(timeLeft - nodeLeft))
)

// IDGenerator 用雪花算法生成唯一的uint64位的ID
type IDGenerator struct {
	mu *sync.Mutex
	lastStamp int64
	nodeID int64
	sequence int64
	epoch time.Time
}

// MakeGenerator 构建一个ID生成器
func MakeGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

// NextID 获取下一个ID
func (w *IDGenerator) NextID() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := time.Since(w.epoch).Nanoseconds() / 1000000
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}

	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = time.Since(w.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		w.sequence = 0
	}
	w.lastStamp = timestamp
	id := (timestamp << timeLeft) | (w.nodeID << nodeLeft) | w.sequence
	return id
}