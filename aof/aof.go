package aof

import (
	"github.com/iverson3/xredis/config"
	"github.com/iverson3/xredis/interface/database"
	"github.com/iverson3/xredis/lib/utils"
	"github.com/iverson3/xredis/redis/connection"
	"github.com/iverson3/xredis/redis/parser"
	"github.com/iverson3/xredis/redis/protocol"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	aofQueueSize = 1 << 16
)

type CmdLine [][]byte

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

type Handler struct {
	db          database.EmbedDB
	tmpDBMaker  func() database.EmbedDB
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	// aof任务结束并准备关闭的时候，通过这个finishedChan给主协程发送结束的信号
	aofFinished chan struct{}
	// aof重写开始和结束的时候需要暂停aof
	pausingAof sync.RWMutex
	currentDB  int
}

func NewAOFHandler(db database.EmbedDB, tmpDBMaker func() database.EmbedDB) (*Handler, error) {
	handler := &Handler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	handler.tmpDBMaker = tmpDBMaker
	// 使用aof文件恢复数据 (依次执行aof文件中所有的命令，将数据加载到内存中)
	handler.LoadAof(0)
	// 以Append的方式打开aof文件
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof 通过channel给aof协程发送命令
func (handler *Handler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		pl := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		handler.aofChan <- pl
	}
}

// 监听aofChan，将收到的命令写入aof文件中
func (handler *Handler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		handler.pausingAof.RLock()

		if handler.currentDB != p.dbIndex {
			// select db
			data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				log.Print(err)
				handler.pausingAof.RUnlock()
				continue
			}
			handler.currentDB = p.dbIndex
		}

		data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			log.Print(err)
		}
		handler.pausingAof.RUnlock()
	}

	// 发送aof任务结束信号
	handler.aofFinished <- struct{}{}
}

// LoadAof 读取aof文件，对aof文件中的命令进行重放，将数据载入内存中
func (handler *Handler) LoadAof(maxBytes int) {
	aofChan := handler.aofChan
	// 暂时置空aofChan 以防止在读取aof文件过程中继续的写入
	handler.aofChan = nil
	defer func(aofChan chan *payload) {
		handler.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		log.Print(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	ch := parser.ParseStream(reader)
	// 只用来保存dbIndex
	fakeConn := &connection.FakeConn{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			log.Printf("parse error: %s\n", p.Err.Error())
			continue
		}

		if p.Data == nil {
			log.Println("empty payload")
			continue
		}

		reply, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			log.Println("required multi bulk protocol")
			continue
		}
		ret := handler.db.Exec(fakeConn, reply.Args)
		if protocol.IsErrorReply(ret) {
			log.Printf("exec error: %v", ret)
		}
	}
}

// Close 优雅的停止aof持久化任务
func (handler *Handler) Close() {
	if handler.aofFile != nil {
		if handler.aofChan != nil {
			close(handler.aofChan)
		}
		// 等待剩余的aof任务处理结束
		<-handler.aofFinished
		err := handler.aofFile.Close()
		if err != nil {
			log.Print(err)
		}
	}
}
