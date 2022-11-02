package connection

import (
	"bytes"
	"net"
	"github.com/iverson3/xredis/lib/sync/wait"
	"sync"
	"time"
)

type Connection struct {
	// 与客户端的Tcp连接
	conn net.Conn

	// 带有timeout功能的WaitGroup，用于优雅的关闭连接
	// 当响应被完整发送前保持waiting状态，阻止连接被关闭
	waitingReply wait.Wait

	// 当服务发送响应数据的时候加锁
	mu sync.Mutex

	password string

	// selected db
	selectedDB int
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

func (c *Connection) SetPassword(passwd string) {
	c.password = passwd
}

func (c *Connection) GetPassword() string {
	return c.password
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}


// FakeConn implements redis.Connection for test
type FakeConn struct {
	Connection
	buf bytes.Buffer
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) error {
	c.buf.Write(b)
	return nil
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.buf.Reset()
}

// Bytes returns written data
func (c *FakeConn) Bytes() []byte {
	return c.buf.Bytes()
}