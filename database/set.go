package database

import (
	"strconv"
	HashSet "studygolang/wangdis/datastruct/set"
	"studygolang/wangdis/interface/database"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/lib/utils"
	"studygolang/wangdis/redis/protocol"
)

// 集合

func (db *DB) getAsSet(key string) (*HashSet.Set, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}

	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, isNew bool, errReply protocol.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return
	}

	if set == nil {
		set = HashSet.Make()
		db.PutEntity(key, &database.DataEntity{Data: set})
		isNew = true
	}
	return
}

func execSAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}

	var count int
	for _, member := range members {
		ret := set.Add(string(member))
		count += ret
	}

	db.addAof(utils.ToCmdLine3("sadd", args...))
	return protocol.MakeIntReply(int64(count))
}

// 判断一个元素是否在指定的集合中
func execSIsMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	member := string(args[1])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}

	exists := set.Has(member)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// 从集合中移除指定的若干元素
func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	members := args[1:]

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		protocol.MakeIntReply(0)
	}

	var count int
	for _, member := range members {
		removed := set.Remove(string(member))
		count += removed
	}
	if set.Len() == 0 {
		db.Remove(key)
	}
	if count > 0 {
		db.addAof(utils.ToCmdLine3("srem", args...))
	}
	return protocol.MakeIntReply(int64(count))
}

// 从集合中随机的移除若干元素 (Set是无序的)
func execSPop(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeArgNumErrReply("spop")
	}
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return &protocol.NullBulkReply{}
	}

	// 没有数量参数，则默认移除一个元素
	count := 1
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || count64 <= 0 {
			return protocol.MakeErrReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}
	if count > set.Len() {
		count = set.Len()
	}

	members := set.RandomDistinctMembers(count)
	results := make([][]byte, len(members))
	for i, member := range members {
		set.Remove(member)
		results[i] = []byte(member)
	}

	if set.Len() == 0 {
		db.Remove(key)
	}
	if count > 0 {
		db.addAof(utils.ToCmdLine3("spop", args...))
	}
	return protocol.MakeMultiBulkReply(results)
}

// 获取集合中元素的数量
func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(set.Len()))
}

// 获取集合中所有的元素
func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	members := make([][]byte, 0, set.Len())
	set.ForEach(func(member string) bool {
		members = append(members, []byte(member))
		return true
	})
	return protocol.MakeMultiBulkReply(members)
}

// 从集合中随机的获取若干不重复的元素
func execSRandMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeArgNumErrReply("srandmember")
	}
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeNullBulkReply()
	}

	if len(args) == 1 {
		members := set.RandomMembers(1)
		return protocol.MakeBulkReply([]byte(members[0]))
	}

	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if count64 == 0 {
		return &protocol.EmptyMultiBulkReply{}
	}
	count := int(count64)

	var members []string
	if count > 0 {
		members = set.RandomDistinctMembers(count)
	} else if count < 0 {
		members = set.RandomMembers(-count)
	}
	results := make([][]byte, 0, len(members))
	for _, member := range members {
		results = append(results, []byte(member))
	}
	return protocol.MakeMultiBulkReply(results)
}

// 求多个集合的交集
func execSInter(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			return &protocol.EmptyMultiBulkReply{}
		}

		if result == nil {
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				return &protocol.EmptyMultiBulkReply{}
			}
		}
	}

	arr := make([][]byte, 0, result.Len())
	result.ForEach(func(member string) bool {
		arr = append(arr, []byte(member))
		return true
	})

	return protocol.MakeMultiBulkReply(arr)
}

// 求多个集合的交集，并将结果存到第一个key里面
func execSInterStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, 0, len(args) - 1)
	keyArgs := args[1:]
	for _, arg := range keyArgs {
		keys = append(keys, string(arg))
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			db.Remove(dest)
			return protocol.MakeIntReply(0)
		}

		if result == nil {
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
		}
	}

	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{Data: set})
	db.addAof(utils.ToCmdLine3("sinterstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

// 求多个集合的并集
func execSUnion(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			continue
		}

		if result == nil {
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	if result == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, 0, result.Len())
	result.ForEach(func(member string) bool {
		arr = append(arr, []byte(member))
		return true
	})

	return protocol.MakeMultiBulkReply(arr)
}

// 求多个集合的并集，并将结果存到第一个key里面
func execSUnionStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, 0, len(args) - 1)
	keyArgs := args[1:]
	for _, arg := range keyArgs {
		keys = append(keys, string(arg))
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			continue
		}

		if result == nil {
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	db.Remove(dest)
	if result == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{Data: set})

	db.addAof(utils.ToCmdLine3("sunionstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

// 求多个集合的差集
func execSDiff(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			if i == 0 {
				return &protocol.EmptyMultiBulkReply{}
			}
			continue
		}

		if result == nil {
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				return &protocol.EmptyMultiBulkReply{}
			}
		}
	}

	if result == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, 0, result.Len())
	result.ForEach(func(member string) bool {
		arr = append(arr, []byte(member))
		return true
	})

	return protocol.MakeMultiBulkReply(arr)
}

// 求多个集合的差集，并将结果存到第一个key里面
func execSDiffStore(db *DB, args [][]byte) redis.Reply {
	dest := string(args[0])
	keys := make([]string, 0, len(args) - 1)
	keyArgs := args[1:]
	for _, arg := range keyArgs {
		keys = append(keys, string(arg))
	}

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			if i == 0 {
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
			continue
		}

		if result == nil {
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
		}
	}

	if result == nil {
		db.Remove(dest)
		return &protocol.EmptyMultiBulkReply{}
	}

	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{Data: set})

	db.addAof(utils.ToCmdLine3("sdiffstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

func init() {
	RegisterCommand("SADD", execSAdd, writeFirstKey, undoSetChange, -3)
	RegisterCommand("SIsMember", execSIsMember, readFirstKey, nil, 3)
	RegisterCommand("SRem", execSRem, writeFirstKey, undoSetChange, -3)
	RegisterCommand("SPop", execSPop, writeFirstKey, undoSetChange, -2)
	RegisterCommand("SCard", execSCard, readFirstKey, nil, 2)
	RegisterCommand("SMembers", execSMembers, readFirstKey, nil, 2)
	RegisterCommand("SRandMember", execSRandMember, readFirstKey, nil, -2)

	RegisterCommand("SInter", execSInter, prepareSetCalculate, nil, -2)
	RegisterCommand("SInterStore", execSInterStore, prepareSetCalculateStore, rollbackFirstKey, -3)

	RegisterCommand("SUnion", execSUnion, prepareSetCalculate, nil, -2)
	RegisterCommand("SUnionStore", execSUnionStore, prepareSetCalculateStore, rollbackFirstKey, -3)

	RegisterCommand("SDiff", execSDiff, prepareSetCalculate, nil, -2)
	RegisterCommand("SDiffStore", execSDiffStore, prepareSetCalculateStore, rollbackFirstKey, -3)
}










