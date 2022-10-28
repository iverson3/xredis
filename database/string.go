package database

import (
	"studygolang/wangdis/interface/database"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/lib/utils"
	"studygolang/wangdis/redis/protocol"
)

// string

func (db *DB) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}

	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

func execSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	val := args[1]

	entity := &database.DataEntity{Data: val}

	ret := db.PutEntity(key, entity)
	if ret > 0 {
		db.addAof(utils.ToCmdLine3("set", args...))
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}

func execGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(bytes)
}

func init() {
	RegisterCommand("Get", execGet, readFirstKey, nil, 2)
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3)
}
