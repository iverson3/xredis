package client

import (
	"log"
	"net"
	"runtime/debug"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/lib/sync/wait"
	"studygolang/wangdis/redis/parser"
	"studygolang/wangdis/redis/protocol"
	"sync"
	"time"
)

const (
	chanSize = 256
	maxWait = 3 * time.Second
)

type Client struct {
	conn net.Conn
	pendingReqs chan *request  // wait to send
	waitingReqs chan *request  // wait for response
	ticker *time.Ticker
	addr string

	working *sync.WaitGroup  // 等待未完成的request结束任务 (发送和接收)
}

type request struct {
	id uint64
	args [][]byte
	reply redis.Reply
	heartbeat bool
	waiting *wait.Wait
	err error
}

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},
	}, nil
}

func (c *Client) Start() {
	c.ticker = time.NewTicker(10 * time.Second)
	go c.handleWrite()
	go func() {
		err := c.handleRead()
		if err != nil {
			log.Printf("error: %v", err)
		}
	}()
	go c.heartbeat()
}

func (c *Client) Close() {
	c.ticker.Stop()

	// 不再接收新的请求
	close(c.pendingReqs)

	// 等待所有处理中的请求完成
	c.working.Wait()

	_ = c.conn.Close()
	close(c.waitingReqs)
}

func (c *Client) handleConnectionError(err error) error {
	err1 := c.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}

	// 如果错误是连接关闭了，则尝试重新建立连接
	conn, err2 := net.Dial("tcp", c.addr)
	if err2 != nil {
		log.Printf("error: %v", err2)
		return err2
	}
	c.conn = conn
	go func() {
		_ = c.handleRead()
	}()
	return nil
}


func (c *Client) heartbeat() {
	for range c.ticker.C {
		c.doHeartbeat()
	}
}

func (c *Client) handleWrite() {
	for req := range c.pendingReqs {
		c.doRequest(req)
	}
}

func (c *Client) Send(args [][]byte) redis.Reply {
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}

	req.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()

	c.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server timeout")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed")
	}
	return req.reply
}

func (c *Client) doHeartbeat() {
	req := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}

	req.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()

	c.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		// heartbeat超时了，则考虑再重试两次？如果仍然超时，则可能是服务器有问题了
	}
}

func (c *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}

	reply := protocol.MakeMultiBulkReply(req.args)
	bytes := reply.ToBytes()
	_, err := c.conn.Write(bytes)
	i := 0
	// 重试3次
	for err != nil && i < 3 {
		err = c.handleConnectionError(err)
		if err == nil {
			_, err = c.conn.Write(bytes)
		}
		i++
	}

	if err == nil {
		c.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (c *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			log.Printf("error: %v", err)
		}
	}()

	req := <-c.waitingReqs
	if req == nil {
		return
	}

	req.reply = reply
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (c *Client) handleRead() error {
	ch := parser.ParseStream(c.conn)
	for payload := range ch {
		if payload.Err != nil {
			c.finishRequest(protocol.MakeErrReply(payload.Err.Error()))
			continue
		}
		c.finishRequest(payload.Data)
	}
	return nil
}