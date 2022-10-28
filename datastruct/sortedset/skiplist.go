package sortedset

import "math/rand"

const (
	maxLevel = 16 // 允许的最大层数
)

type Element struct {
	Member string  // key
	Score  float64 // 排序的分值
}

type Level struct {
	forward *node // 指向后一个节点
	span    int64 // 从当前层找到下一个节点时，跳过的节点数
}

type node struct {
	Element
	backward *node    // 指向前一个节点
	level    []*Level // level[0]是最下面的一层
}

type skiplist struct {
	header *node
	tail   *node
	length int64 // 节点数量
	level  int16 // 层数最高的那个节点的层数
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		header: makeNode(maxLevel, 0, ""),
		level:  1,
	}
}

// 随机生成一个层数
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level > maxLevel {
		return maxLevel
	}
	return level
}

// 向跳表中插入一个节点
func (skip *skiplist) insert(member string, score float64) *node {
	if skip == nil {
		panic("skiplist is nil")
	}

	// 寻找新节点的先驱节点，它们的 forward 将指向新节点
	// 存放每一层需要在插入节点后更新的节点(先驱节点)
	update := make([]*node, maxLevel)
	// 计算存储需要修改调整的跳过节点数 (还不太清楚)
	// 保存各层先驱节点的排名，用于计算span
	rank := make([]int64, maxLevel)

	// 找到新节点插入的位置 (遍历结束之后 node即是新节点插入位置的前一个节点)
	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		if node.level[i] != nil {
			// 新节点的score大于当前节点的score则继续往后寻找，或者 score相等则判断member的大小(即key的大小)
			for node.level[i].forward != nil && (node.level[i].forward.Score < score || (node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	// 为新节点生成的随机的level
	newLevel := randomLevel()

	// 更新最大的level
	if newLevel > skip.level {
		for i := skip.level; i < newLevel; i++ {
			rank[i] = 0
			update[i] = skip.header
			update[i].level[i].span = skip.length
		}
		skip.level = newLevel
	}

	// 构建一个新节点
	newNode := makeNode(newLevel, score, member)
	// 依次调整每一层向后的指针以及跳过的节点数
	for i := 0; i < int(newLevel); i++ {
		newNode.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = newNode

		// todo: 不太清楚span的计算逻辑
		newNode.level[i].span = 0
	}

	// 调整新节点向前的指针
	if node == skip.header {
		newNode.backward = nil
	} else {
		newNode.backward = node
	}

	if newNode.level[0].forward == nil {
		// 如果新节点是最后一个节点，则更新跳表的tail指针
		skip.tail = newNode
	} else {
		// 将新节点设置为其后节点的前一个节点
		newNode.level[0].forward.backward = newNode
	}

	skip.length++

	return newNode
}

func (skip *skiplist) removeNode(node *node, update []*node) {
	for i := 0; i < int(skip.level); i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		skip.tail = node.backward
	}

	// 更新skiplist的最高层数level
	for skip.level > 1 && skip.header.level[skip.level-1].forward == nil {
		skip.level--
	}
	node = nil
	skip.length--
}

func (skip *skiplist) remove(member string, score float64) bool {
	// 存放所有需要在删除节点后更新的节点
	update := make([]*node, maxLevel)

	// 找到待移除节点所在的位置 (遍历结束之后 node即为待移除节点的前一个节点)
	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		if node.level[i].forward != nil && (node.level[i].forward.Score < score || (node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}

	// 下面代码执行之后，node即为待移除的节点 (只是理论上的待移除节点，但不一定符合传入的参数)
	node = node.level[0].forward
	if node != nil && node.Score == score && node.Member == member {
		skip.removeNode(node, update)
		return true
	}

	// 到这说明没有找到待移除的节点
	return false
}

func (skip *skiplist) getRank(member string, score float64) int64 {
	var rank int64
	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (node.level[i].forward.Score < score || (node.level[i].forward.Score == score && node.level[i].forward.Member <= member)) {
			rank += node.level[i].span
			node = node.level[i].forward
		}

		if node.Member == member {
			return rank
		}
	}
	return 0
}

func (skip *skiplist) getByRank(rank int64) *node {
	var span int64
	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (span+node.level[i].span) <= rank {
			span += node.level[i].span
			node = node.level[i].forward
		}

		if span == rank {
			return node
		}
	}
	return nil
}

func (skiplist *skiplist) hasInRange() {

}
