package cluster

import "github.com/iverson3/xredis/interface/redis"

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)

	// list
	routerMap["lpush"] = defaultFunc
	routerMap["lpop"] = defaultFunc
	routerMap["llen"] = defaultFunc
	routerMap["lrange"] = defaultFunc

	// string
	routerMap["set"] = defaultFunc
	routerMap["get"] = defaultFunc

	// set

	// hash

	// zset

	return routerMap
}

func defaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[0])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, args)
}