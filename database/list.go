package database

import (
	"strconv"
	List "studygolang/wangdis/datastruct/list"
	"studygolang/wangdis/interface/database"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/lib/utils"
	"studygolang/wangdis/redis/protocol"
)

func (db *DB) getAsList(key string) (*List.LinkedList, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	linkedList, ok := entity.Data.(*List.LinkedList)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return linkedList, nil
}

func (db *DB) getOrInitList(key string) (list *List.LinkedList, isNew bool, errReply protocol.ErrorReply) {
	list, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}

	if list == nil {
		list = &List.LinkedList{}
		db.PutEntity(key, &database.DataEntity{Data: list})
		isNew = true
	}
	return
}

// 根据参数从链表中获取对应的值
func execLIndex(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.NullBulkReply{}
	}

	size := list.Len()
	if index < -1*size {
		return &protocol.NullBulkReply{}
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return &protocol.NullBulkReply{}
	}

	val, _ := list.Get(index).([]byte)
	return protocol.MakeBulkReply(val)
}

// 获取链表的长度
func execLLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	size := int64(list.Len())
	return protocol.MakeIntReply(size)
}

// 移除链表中的第一个节点，并且返回该节点
func execLPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.NullBulkReply{}
	}

	valInter := list.Remove(0)
	if list.Len() == 0 {
		db.Remove(key)
	}
	val, _ := valInter.([]byte)

	db.addAof(utils.ToCmdLine3("lpop", args...))
	return protocol.MakeBulkReply(val)
}

var lPushCmd = []byte("LPUSH")

// LPop命令的回滚命令
func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(0).([]byte)
	return []CmdLine{
		{
			lPushCmd,
			args[0],
			element,
		},
	}
}

// 向链表头部插入若干元素，并返回插入后总元素的个数
func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, val := range values {
		list.Insert(0, val)
	}

	db.addAof(utils.ToCmdLine3("lpush", args...))
	return protocol.MakeIntReply(int64(list.Len()))
}

// LPush命令的回滚命令
func undoLPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	valCount := len(args) - 1
	cmdLines := make([]CmdLine, 0, valCount)
	for i := 0; i < valCount; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("LPOP", key))
	}
	return cmdLines
}

// 向链表头部插入若干元素(只有当链表存在才插入)，并返回插入后总元素的个数
func execLPushX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	for _, val := range values {
		list.Insert(0, val)
	}

	db.addAof(utils.ToCmdLine3("lpushx", args...))
	return protocol.MakeIntReply(int64(list.Len()))
}

// 从链表中获取指定区间内的所有元素
func execLRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop := int(stop64)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := list.Len()
	if size <= 0 {
		return &protocol.EmptyMultiBulkReply{}
	}

	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}

	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}

	if stop < start {
		stop = start
	}

	slice := list.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}
	return protocol.MakeMultiBulkReply(result)
}

// 从链表中移除指定位置的元素
func execLRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(value)
	} else if count > 0 {
		removed = list.RemoveByVal(value, count)
	} else {
		removed = list.ReverseRemoveByVal(value, -count)
	}

	if list.Len() == 0 {
		db.Remove(key)
	}

	if removed > 0 {
		db.addAof(utils.ToCmdLine3("lrem", args...))
	}
	return protocol.MakeIntReply(int64(removed))
}

// 向链表中指定位置插入元素
func execLSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeErrReply("ERR no such key")
	}

	size := list.Len()
	if index < -1*size {
		return protocol.MakeErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return protocol.MakeErrReply("ERR index out of range")
	}

	list.Set(index, value)
	db.addAof(utils.ToCmdLine3("lset", args...))
	return &protocol.OkReply{}
}

// LSet命令的回滚命令
func undoLSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil
	}
	index := int(index64)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil {
		return nil
	}

	size := list.Len()
	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}

	// 获取旧值
	value, _ := list.Get(index).([]byte)
	return []CmdLine{
		{
			[]byte("LSET"),
			args[0],
			args[1],
			value,
		},
	}
}

// 移除链表中最后一个元素，并返回该元素
func execRPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.NullBulkReply{}
	}

	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLine3("rpop", args...))
	return protocol.MakeBulkReply(val)
}

var rPushCmd = []byte("RPUSH")

// RPop命令的回滚命令
func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}

	// 获取旧值
	val, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			val,
		},
	}
}

func prepareRPopLPush(args [][]byte) ([]string, []string) {
	return []string{
		string(args[0]),
		string(args[1]),
	}, nil
}

// 将A链表中最后一个元素拿出来，插入到B链表的头部
func execRPopLPush(db *DB, args [][]byte) redis.Reply {
	sourceKey := string(args[0])
	destKey := string(args[1])

	sourceList, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return &protocol.NullBulkReply{}
	}

	destList, _, errReply := db.getOrInitList(destKey)
	if errReply != nil {
		return errReply
	}

	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.Remove(sourceKey)
	}

	db.addAof(utils.ToCmdLine3("rpoplpush", args...))
	return protocol.MakeBulkReply(val)
}

// RPopLPush命令的回滚命令
func undoRPopLPush(db *DB, args [][]byte) []CmdLine {
	sourceKey := string(args[0])
	list, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}

	val, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			val,
		},
		{
			[]byte("LPOP"),
			args[1],
		},
	}
}

// 往链表尾部插入多个元素
func execRPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, val := range values {
		list.Add(val)
	}

	db.addAof(utils.ToCmdLine3("rpush", args...))
	return protocol.MakeIntReply(int64(list.Len()))
}

// RPush命令的回滚命令
func undoRPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)

	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("RPOP", key))
	}
	return cmdLines
}

// 只有当链表存在的时候，才往链表尾部插入多个元素
func execRPushX(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	for _, val := range values {
		list.Add(val)
	}

	db.addAof(utils.ToCmdLine3("rpushx", args...))
	return protocol.MakeIntReply(int64(list.Len()))
}

func init() {
	RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3)
	RegisterCommand("LLen", execLLen, readFirstKey, nil, 2)
	RegisterCommand("LPop", execLPop, writeFirstKey, undoLPop, 2)
	RegisterCommand("LPush", execLPush, writeFirstKey, undoLPush, -3)
	RegisterCommand("LPushX", execLPushX, writeFirstKey, undoLPush, -3)
	RegisterCommand("LRange", execLRange, readFirstKey, nil, 4)
	RegisterCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4)
	RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4)
	RegisterCommand("RPop", execRPop, writeFirstKey, undoRPop, 2)
	RegisterCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3)
	RegisterCommand("RPush", execRPush, writeFirstKey, undoRPush, -3)
	RegisterCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3)
}
