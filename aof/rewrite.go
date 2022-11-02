package aof

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"github.com/iverson3/xredis/config"
	"github.com/iverson3/xredis/interface/database"
	"github.com/iverson3/xredis/lib/utils"
	"github.com/iverson3/xredis/redis/protocol"
	"time"
)

func (handler *Handler) newRewriteHandler() *Handler {
	h := &Handler{}
	h.aofFilename = handler.aofFilename
	h.db = handler.tmpDBMaker()
	return h
}

type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite aof rewrite实现
func (handler *Handler) Rewrite() error {
	ctx, err := handler.StartRewrite()
	if err != nil {
		return err
	}

	err = handler.DoRewrite(ctx)
	if err != nil {
		return err
	}

	handler.FinishRewrite(ctx)
	return nil
}

func (handler *Handler) StartRewrite() (*RewriteCtx, error) {
	// 暂停aof处理任务
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()

	err := handler.aofFile.Sync()
	if err != nil {
		log.Println("fsync failed")
		return nil, err
	}

	// 获取当前aof文件大小
	fileInfo, err := os.Stat(handler.aofFilename)
	if err != nil {
		return nil, err
	}

	// 创建临时文件
	tmpFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		log.Println("tmp file create failed")
		return nil, err
	}

	return &RewriteCtx{
		tmpFile:  tmpFile,
		fileSize: fileInfo.Size(),
		dbIdx:    handler.currentDB,
	}, nil
}

// DoRewrite aof重写期间是允许aof任务正常执行的 (在不阻塞在线服务的同时进行其它操作是一项必需的能力，AOF重写的思路在解决这类问题时具有重要的参考价值)
func (handler *Handler) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// 加载aof临时文件
	tmpAofHandler := handler.newRewriteHandler()
	tmpAofHandler.LoadAof(int(ctx.fileSize))

	// 重写aof临时文件
	for i := 0; i < config.Properties.Databases; i++ {
		// 选择数据库
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}

		// 将对应内存数据库中的所有数据转存到临时aof文件中
		tmpAofHandler.db.ForEach(i, func(key string, data *database.DataEntity, expiration *time.Time) bool {
			// 将内存数据库中的数据实体转成对应的命令，再写入临时aof文件
			cmd := EntityToCmd(key, data)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}

			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

func (handler *Handler) FinishRewrite(ctx *RewriteCtx) {
	// 暂停aof任务
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	// 将aof重写期间生成并写入aof文件中的命令写入aof临时文件中
	// 打开aof文件
	src, err := os.Open(handler.aofFilename)
	if err != nil {
		log.Printf("open aofFilename failed, error: %v", err)
		return
	}
	defer func() {
		_ = src.Close()
	}()

	// 将文件读写位置移动到aof重写涉及的位置
	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		log.Printf("seek failed, error: %v", err)
		return
	}

	// 将aof临时文件的dbIndex设置为跟aof文件的dbIndex一致
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		log.Printf("tmp file rewrite failed, error: %v", err)
		return
	}

	// 拷贝数据
	// 将aof文件中从读写位置往后的所有数据拷贝到aof临时文件中去(append)
	_, err = io.Copy(tmpFile, src)
	if err != nil {
		log.Printf("copy aof failed, error: %v", err)
		return
	}

	// 将当前的aof文件关闭(旧的aof文件)
	_ = handler.aofFile.Close()
	// 使用aof临时文件替换掉当前的aof文件
	_ = os.Rename(tmpFile.Name(), handler.aofFilename)

	// 重新打开aof文件(替换之后的aof文件)，以便之后的aof任务使用
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	handler.aofFile = aofFile

	// 重新写入一次select dbIndex命令，确保aof中的数据库与handler.currentDB一致
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDB))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
