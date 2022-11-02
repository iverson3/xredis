package database

import (
	"github.com/iverson3/xredis/config"
	"github.com/iverson3/xredis/interface/redis"
	"github.com/iverson3/xredis/redis/protocol"
)

// Auth 校验客户端的密码
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("auth")
	}

	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR client send AUTH, but no password is set")
	}

	passwd := string(args[0])
	c.SetPassword(passwd)

	if config.Properties.RequirePass != passwd {
		return protocol.MakeErrReply("ERR invalid password")
	}
	return &protocol.OkReply{}
}
