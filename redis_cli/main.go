package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// redis客户端
// 提供终端给用户输入redis命令，连接redis服务，发送命令并获取结果

const (
	cmdPre = "redis-cli[%d]>"
	delim  = '\n'
	CRLF   = "\r\n"
)

func readMsg(conn *net.Conn, readFinished chan struct{}) {
	reader := bufio.NewReader(*conn)
	status := true
	count := 0

	for {
		if status {
			buf, err := reader.ReadBytes(delim)
			if err != nil {
				if err == os.ErrClosed || err == io.ErrClosedPipe {
					return
				}
				continue
			}

			if buf[len(buf)-2] != '\r' {
				continue
			}
			// 移除\r\n
			buf = buf[:len(buf)-2]

			if buf[0] == '$' {
				// $开头的结果，表示多行消息，$后面是下一行数据的长度
				count, err = strconv.Atoi(string(buf[1:]))
				if err != nil {
					continue
				}
				if count == -1 {
					// 示例：$-1
					fmt.Printf("%d\n", count)
					readFinished <- struct{}{}
					count = 0
					continue
				}
				status = false
				continue
			} else if buf[0] == ':' {
				// :开头的结果，表示数字
				fmt.Printf("%s\n", string(buf[1:]))
				readFinished <- struct{}{}
				continue
			} else if buf[0] == '+' {
				// +OK
				fmt.Printf("%s\n", string(buf[1:]))
				readFinished <- struct{}{}
			} else if buf[0] == '-' {
				// -ERR error info
				fmt.Printf("ERROR: %s\n", string(buf[1:]))
				readFinished <- struct{}{}
			} else if buf[0] == '*' {
				// todo: xxx
			} else {
				fmt.Printf("%s\n", string(buf))
				readFinished <- struct{}{}
			}
		} else {
			if count == 0 {
				status = true
				continue
			}
			buf := make([]byte, count+2)
			_, err := io.ReadFull(reader, buf)
			if err != nil {
				continue
			}

			buf = buf[:len(buf)-2]

			fmt.Printf("%s\n", string(buf))

			status = true
			count = 0
			readFinished <- struct{}{}
		}
	}
}

func main() {
	var cmdStr string
	var err error
	var dbIndex int // 当前操作的数据库，默认为0
	// 发送携程和读取携程之间同步的通道
	readFinished := make(chan struct{})

	// 连接redis服务
	conn, err := net.Dial("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 启动读取携程
	go readMsg(&conn, readFinished)

	reader := bufio.NewReader(os.Stdin)
	for {
		cmdStr = ""
		args := make([][]byte, 0)

		// 输出提示语
		fmt.Printf(cmdPre, dbIndex)

		cmdStr, err = reader.ReadString(delim)
		if err != nil {
			panic(err)
		}
		if cmdStr == "" || cmdStr[0] == delim || cmdStr == CRLF {
			continue
		}

		// 移除末尾的 \n 或 \r\n
		cmdStr = strings.TrimRight(cmdStr, "\n")
		cmdStr = strings.TrimRight(cmdStr, "\r")

		if cmdStr == "exit" {
			return
		}

		// 切割命令参数
		cmdStrSlice := strings.Split(cmdStr, " ")
		if cmdStrSlice[0] == "select" {
			if len(cmdStrSlice) != 2 {
				fmt.Printf("invalid select cmd: %s\n", cmdStr)
				continue
			}
			index, err := strconv.Atoi(cmdStrSlice[1])
			if err != nil {
				fmt.Printf("invalid dbIndex: %s\n", cmdStrSlice[1])
				continue
			}
			dbIndex = index
		}
		for _, cmd := range cmdStrSlice {
			cmd = fmt.Sprintf("%s%s", cmd, CRLF)
			args = append(args, []byte(cmd))
		}
		//fmt.Println(args)

		// 组装数据 向redis服务端发送命令数据
		cmdBytes := make([]byte, 0)
		count := fmt.Sprintf("*%d%s", len(args), CRLF)
		cmdBytes = append(cmdBytes, []byte(count)...)

		for _, arg := range args {
			argLen := fmt.Sprintf("$%d%s", len(arg)-2, CRLF)
			cmdBytes = append(cmdBytes, []byte(argLen)...)
			cmdBytes = append(cmdBytes, arg...)
		}

		_, err = conn.Write(cmdBytes)
		if err != nil {
			panic(err)
		}

		// 等待读取携程读到redis对于本次命令的完整响应再进入下一轮
		<-readFinished
	}
}
