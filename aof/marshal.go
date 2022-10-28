package aof

import (
	"strconv"
	"studygolang/wangdis/datastruct/dict"
	List "studygolang/wangdis/datastruct/list"
	"studygolang/wangdis/datastruct/set"
	"studygolang/wangdis/datastruct/sortedset"
	"studygolang/wangdis/interface/database"
	"studygolang/wangdis/redis/protocol"
	"time"
)

func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}

	var cmd *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case *List.LinkedList:
		cmd = listToCmd(key, val)
	case *set.Set:
	case dict.Dict:
	case *sortedset.SortedSet:
	}
	return cmd
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, list *List.LinkedList) *protocol.MultiBulkReply {
	args := make([][]byte, list.Len()+2)
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[i+2] = bytes
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var pExpireAtBytes = []byte("PEXPIREAT")

func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
