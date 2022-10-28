package dict

type SimpleDict struct {
	m map[string]interface{}
}

func (dict *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, exists = dict.m[key]
	return
}

func (dict *SimpleDict) Len() int {
	if dict.m == nil {
		panic("dict.m is nil")
	}
	return len(dict.m)
}

func (dict *SimpleDict) Put(key string, val interface{}) (result int) {
	_, exists := dict.m[key]
	dict.m[key] = val
	if exists {
		return 0
	}
	return 1
}

func (dict *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, exists := dict.m[key]
	if exists {
		return 0
	}
	dict.m[key] = val
	return 1
}

func (dict *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	_, exists := dict.m[key]
	if !exists {
		return 0
	}

	dict.m[key] = val
	return 1
}

func (dict *SimpleDict) Remove(key string) (result int) {
	_, exists := dict.m[key]
	if !exists {
		return 0
	}

	delete(dict.m, key)
	return 1
}

func (dict *SimpleDict) ForEach(consumer Consumer) {
	for key, val := range dict.m {
		if !consumer(key, val) {
			break
		}
	}
}

func (dict *SimpleDict) Keys() []string {
	keys := make([]string, 0, len(dict.m))
	for key := range dict.m {
		keys = append(keys, key)
	}
	return keys
}

func (dict *SimpleDict) RandomKeys(limit int) []string {
	keys := make([]string, 0, limit)
	var count int
	for key := range dict.m {
		keys = append(keys, key)
		count++
		if count == limit {
			break
		}
	}
	return keys
}

func (dict *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(dict.m) {
		size = len(dict.m)
	}

	keys := make([]string, 0, size)
	var count int
	for key := range dict.m {
		keys = append(keys, key)
		count++
		if count == limit {
			break
		}
	}
	return keys
}

func (dict *SimpleDict) Clear() {
	*dict = *MakeSimple()
}

func MakeSimple() *SimpleDict {
	return &SimpleDict{m: make(map[string]interface{})}
}

