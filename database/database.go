package database

import (
	"fmt"
	"github.com/iverson3/xredis/aof"
	"github.com/iverson3/xredis/config"
	"github.com/iverson3/xredis/interface/database"
	"github.com/iverson3/xredis/interface/redis"
	"github.com/iverson3/xredis/lib/utils"
	"github.com/iverson3/xredis/redis/protocol"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

type MultiDB struct {
	dbSet []*DB

	aofHandler *aof.Handler
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}

	var validAof bool
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})
		if err != nil {
			panic(err)
		}

		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			singleDB := db
			singleDB.addAof = func(cmdLine CmdLine) {
				mdb.aofHandler.AddAof(singleDB.index, cmdLine)
			}
		}
		validAof = true
	}

	if config.Properties.RDBFilename != "" && !validAof {
		// todo: load rdb
		//loadRdb(mdb)
	}
	return mdb
}

func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		mdb.dbSet[i] = makeBasicDB()
	}
	return mdb
}

func (mdb *MultiDB) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// 对于cmdName是特殊命令时的判断和处理
	// 1.检验权限
	// 2.特殊命令的单独处理 (不能在事务中执行的特殊命令，subscribe publish flushall等)
	if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "flushdb" {
		return mdb.flushDB(c)
	} else if cmdName == "flushall" {
		return mdb.flushAll()
	} else if cmdName == "select" {
		return execSelect(mdb, c, cmdLine)
	}

	// todo: support multi database transaction

	// 之后则是普通命令的执行
	dbIndex := c.GetDBIndex()
	if dbIndex >= len(mdb.dbSet) {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// AfterClientClose does some clean after client close connection
func (mdb *MultiDB) AfterClientClose(c redis.Connection) {
	// todo: AfterClientClose
}

func (mdb *MultiDB) Close() {
	if mdb.aofHandler != nil {
		mdb.aofHandler.Close()
	}
}

// 根据客户端的命令设置当前数据库为对应的编号
func execSelect(mdb *MultiDB, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("select")
	}
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR DB index is invalid")
	}
	if index >= len(mdb.dbSet) || index < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(index)

	//if mdb.aofHandler != nil {
	//	mdb.aofHandler.AddAof(index, utils.ToCmdLine("SELECT", strconv.Itoa(index)))
	//}
	return protocol.MakeOkReply()
}

// 选择指定编号的数据库
func (mdb *MultiDB) selectDB(dbIndex int) *DB {
	if dbIndex >= len(mdb.dbSet) {
		panic("ERR DB index is out of range")
	}
	return mdb.dbSet[dbIndex]
}

// 清空当前数据库
func (mdb *MultiDB) flushDB(c redis.Connection) redis.Reply {
	mdb.dbSet[c.GetDBIndex()].Flush()

	if mdb.aofHandler != nil {
		mdb.aofHandler.AddAof(c.GetDBIndex(), utils.ToCmdLine("FlushDB", strconv.Itoa(c.GetDBIndex())))
	}
	return &protocol.OkReply{}
}

// 清空所有数据库
func (mdb *MultiDB) flushAll() redis.Reply {
	for _, db := range mdb.dbSet {
		db.Flush()
	}

	if mdb.aofHandler != nil {
		mdb.aofHandler.AddAof(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

func (mdb *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	//TODO implement me
	panic("implement me")
}

func (mdb *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []database.CmdLine) redis.Reply {
	//TODO implement me
	panic("implement me")
}

func (mdb *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []database.CmdLine {
	//TODO implement me
	panic("implement me")
}

func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	mdb.dbSet[dbIndex].ForEach(cb)
}

func (mdb *MultiDB) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	//TODO implement me
	panic("implement me")
}

func (mdb *MultiDB) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	//TODO implement me
	panic("implement me")
}

func (mdb *MultiDB) GetDBSize(dbIndex int) (int, int) {
	//TODO implement me
	panic("implement me")
}

// BGRewriteAOF 在后台异步的执行aof重写
func BGRewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	if db.aofHandler == nil {
		return protocol.MakeErrReply("aof is not enabled")
	}
	go db.aofHandler.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF 同步的执行aof重写
func RewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	if db.aofHandler == nil {
		return protocol.MakeErrReply("aof is not enabled")
	}
	err := db.aofHandler.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}
