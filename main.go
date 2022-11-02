package main

import (
	"github.com/iverson3/xredis/redis/server"
	"github.com/iverson3/xredis/tcp"
)

func main() {
	tcp.ListenAndServe(&tcp.Config{Address: ":9000"}, server.MakeHandler())
}
