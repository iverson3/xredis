package main

import (
	"studygolang/wangdis/redis/server"
	"studygolang/wangdis/tcp"
)

func main() {
	tcp.ListenAndServe(&tcp.Config{Address: ":9000"}, server.MakeHandler())
}
