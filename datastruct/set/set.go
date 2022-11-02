package set

import "github.com/iverson3/xredis/datastruct/dict"

type Set struct {
	dict dict.Dict
}

func Make(members ...string) *Set {
	set := &Set{dict: dict.MakeSimple()}
	for _, m := range members {
		set.Add(m)
	}
	return set
}

func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

func (set *Set) Remove(val string) int {
	return set.dict.Remove(val)
}

func (set *Set) Has(val string) bool {
	_, exists := set.dict.Get(val)
	return exists
}

func (set *Set) Len() int {
	return set.dict.Len()
}

func (set *Set) ToSlice() []string {
	slice := make([]string, 0, set.Len())
	set.dict.ForEach(func(key string, val interface{}) bool {
		slice = append(slice, key)
		return true
	})
	return slice
}

func (set *Set) ForEach(consumer func(member string) bool) {
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// Intersect 求两个Set的交集
func (set *Set) Intersect(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	set.ForEach(func(member string) bool {
		if another.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// Union 求两个Set的并集
func (set *Set) Union(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	another.ForEach(func(member string) bool {
		if !result.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// Diff 求两个Set的差集
func (set *Set) Diff(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	set.ForEach(func(member string) bool {
		if !another.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// RandomMembers 随机的获取指定数量的元素
func (set *Set) RandomMembers(limit int) []string {
	return set.dict.RandomKeys(limit)
}

// RandomDistinctMembers 随机的获取指定数量且不重复的若干元素
func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}
