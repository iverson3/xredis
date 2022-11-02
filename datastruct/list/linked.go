package list

import "github.com/iverson3/xredis/lib/utils"

// LinkedList 双向链表
type LinkedList struct {
	first *node
	last *node
	size int
}

type node struct {
	val interface{}
	prev *node
	next *node
}

func (list *LinkedList) Add(val interface{}) {
	if list == nil {
		panic("list is nil")
	}

	n := &node{val: val}
	// 判断是否为空链表
	if list.last == nil {
		list.first = n
		list.last = n
	} else {
		// 将新节点追加到链表的最后
		n.prev = list.last
		list.last.next = n
		list.last = n
	}
	list.size++
}

func (list *LinkedList) find(index int) (n *node) {
	if index < list.size / 2 {
		n = list.first
		for i := 0; i < index; i++ {
			n = n.next
		}
	} else {
		n = list.last
		for i := list.size - 1; i > index; i-- {
			n = n.prev
		}
	}
	return
}

func (list *LinkedList) Get(index int) (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	return list.find(index).val
}

func (list *LinkedList) Set(index int, val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index > list.size {
		panic("index out of bound")
	}
	n := list.find(index)
	n.val = val
}

func (list *LinkedList) Insert(index int, val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index > list.size {
		panic("index out of bound")
	}
	if index == list.size {
		 list.Add(val)
		return
	}

	n := list.find(index)
	newNode := &node{
		val:  val,
		prev: n.prev,
		next: n,
	}
	if n.prev != nil {
		n.prev.next = newNode
	}  else {
		list.first = newNode
	}
	n.prev = newNode
	list.size++
}

func (list *LinkedList) removeNode(n *node) {
	// 当前节点的前一个节点为空，则表示当前节点为首节点
	if n.prev == nil {
		list.first = n.next
	} else {
		n.prev.next = n.next
	}
	// 当前节点的后一个节点为空，则表示当前节点为尾节点
	if n.next == nil {
		list.last = n.prev
	} else {
		n.next.prev = n.prev
	}

	// 清除引用，便于GC回收
	n.prev = nil
	n.next = nil

	list.size--
}

func (list *LinkedList) Remove(index int) (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index > list.size {
		panic("index out of bound")
	}

	n := list.find(index)
	list.removeNode(n)
	return n.val
}

func (list *LinkedList) RemoveLast() (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	// 尾节点为空，则表示是空链表
	if list.last == nil {
		return nil
	}

	n := list.last
	list.removeNode(n)
	return n.val
}

func (list *LinkedList) RemoveAllByVal(val interface{}) int {
	if list == nil {
		panic("list is nil")
	}

	n := list.first
	var nextNode *node
	removed := 0
	for n != nil {
		// 将当前节点的下一个节点暂存起来，因为接下来当前节点可能会被移除，移除后就找不到下一个节点了
		nextNode = n.next
		if utils.Equals(n.val, val) {
			list.removeNode(n)
			removed++
		}
		n = nextNode
	}
	return removed
}

// RemoveByVal 从前往后遍历比较并移除指定数量的节点
func (list *LinkedList) RemoveByVal(val interface{}, count int) int {
	if list == nil {
		panic("list is nil")
	}

	n := list.first
	var nextNode *node
	removed := 0
	for n != nil {
		// 将当前节点的下一个节点暂存起来，因为接下来当前节点可能会被移除，移除后就找不到下一个节点了
		nextNode = n.next
		if utils.Equals(n.val, val) {
			list.removeNode(n)
			removed++
		}
		if removed == count {
			break
		}
		n = nextNode
	}
	return removed
}

// ReverseRemoveByVal 从后往前遍历比较并移除指定数量的节点
func (list *LinkedList) ReverseRemoveByVal(val interface{}, count int) int {
	if list == nil {
		panic("list is nil")
	}

	n := list.last
	var prevNode *node
	removed := 0
	for n != nil {
		// 将当前节点的上一个节点暂存起来，因为接下来当前节点可能会被移除，移除后就找不到上一个节点了
		prevNode = n.prev
		if utils.Equals(n.val, val) {
			list.removeNode(n)
			removed++
		}
		if removed == count {
			break
		}
		n = prevNode
	}
	return removed
}

func (list *LinkedList) Len() int {
	if list == nil {
		panic("list is nil")
	}
	return list.size
}

// ForEach 遍历链表中的每一个元素
func (list *LinkedList) ForEach(consumer func(int, interface{}) bool) {
	if list == nil {
		panic("list is nil")
	}

	n := list.first
	i := 0
	for n != nil {
		goNext := consumer(i, n.val)
		// 返回false 则终止循环
		if !goNext {
			break
		}
		i++
		n = n.next
	}
}

func (list *LinkedList) Contains(val interface{}) bool {
	if list == nil {
		panic("list is nil")
	}

	var exists bool
	list.ForEach(func(i int, v interface{}) bool {
		if utils.Equals(v, val) {
			exists = true
			return false
		}
		return true
	})
	return exists
}

func (list *LinkedList) Range(start, stop int) []interface{} {
	if list == nil {
		panic("list is nil")
	}
	if start < 0 || start >= list.size {
		panic("index out of bound")
	}
	if stop < start || stop > list.size {
		panic("index out of bound")
	}

	sliceSize := stop - start
	slice := make([]interface{}, sliceSize)

	i := 0
	n := list.first
	for n != nil {
		if i >= start && i < stop {
			slice = append(slice, n.val)
		} else if i >= stop {
			break
		}

		i++
		n = n.next
	}
	return slice
}

func Make(vals ...interface{}) *LinkedList {
	list := LinkedList{}
	for _, val := range vals {
		list.Add(val)
	}
	return &list
}














