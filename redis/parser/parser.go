package parser

import (
	"bufio"
	"errors"
	"io"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/redis/protocol"
)

type readState struct {
	readingMultiLine bool
	expectedArgsCount int
	msgType byte
	args [][]byte
	bulkLen int64   // 将要读取的 BulkString的正文长度
}

func (r readState) finished() bool {
	return r.expectedArgsCount > 0 && len(r.args) == r.expectedArgsCount
}

type Payload struct {
	Data redis.Reply
	Err error
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go doParse(reader, ch)
	return ch
}

func doParse(reader io.Reader, ch chan<- *Payload)  {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(string(debug.Stack()))
		}
	}()

	var state readState
	var err error
	var msg []byte
	bufReader := bufio.NewReader(reader)

	for {
		// 读取一行数据
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			// if is ioErr, stop read then return
			if ioErr {
				ch <- &Payload{
					Err:  err,
				}
				close(ch)
				return
			}

			// protocol error， reset read state then continue
			ch <- &Payload{
				Err:  err,
			}
			state = readState{}
			continue
		}

		// 解析一行数据
		if !state.readingMultiLine {
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err:  errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}

				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data:  &protocol.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err:  errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}

				if state.bulkLen == -1 {
					ch <- &Payload{
						Data:  &protocol.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err:  errors.New("protocol error: " + string(msg)),
				}
				state = readState{}
				continue
			}

			// 接收到一个完整的redis请求消息
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeBulkReply(state.args[0])
				}

				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 {
		// 读取正常的一行数据
		// 正常模式下使用 \r\n 区分数据行
		// 读取 *3\r\n 或 $5\r\n
		msg, err = bufReader.ReadBytes('\n')
		// 判断读到的数据是否以 \r\n 结尾
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg) - 2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 当读取到BulkString第二行时，根据给出的长度进行读取
		msg = make([]byte, state.bulkLen + 2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		// 判断读到的数据是否以 \r\n 结尾
		if len(msg) == 0 || msg[len(msg) - 2] != '\r' || msg[len(msg) - 1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		// BulkString读取完毕，重新使用正常模式
		state.bulkLen = 0
	}
	return msg, false, nil
}

// 解析 *3\r\n
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// 解析 $3\r\n
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {  // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+':
		result = protocol.MakeStatusReply(str[1:])
	case '-':
		result = protocol.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = protocol.MakeIntReply(val)
	default:
		arr := strings.Split(str, " ")
		args := make([][]byte, len(arr))
		for i := range arr {
			args[i] = []byte(arr[i])
		}
		result = protocol.MakeMultiBulkReply(args)
	}
	return result, nil
}

func readBody(msg []byte, state *readState) error {
	line := msg[:len(msg)-2]    // 移除换行符 \r\n
	var err error
	if line[0] == '$' {
		// 解析 $5\r\n
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}

		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}