package cluster

import (
	"context"
	"github.com/jolestar/go-commons-pool/v2"
	"log"
	"runtime/debug"
	"strings"
	"studygolang/wangdis/config"
	"studygolang/wangdis/database"
	"studygolang/wangdis/datastruct/dict"
	databaseInter "studygolang/wangdis/interface/database"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/lib/consistenthash"
	"studygolang/wangdis/lib/idgenerator"
	"studygolang/wangdis/redis/protocol"
)

const (
	replicas = 3
)

type PeerPicker interface {
	AddNode(keys ...string)
	PickNode(key string) string
}

// Cluster 代表redis集群的一个节点
// 将命令发到集群的其他节点或协调其他节点以完成事务
type Cluster struct {
	self string

	nodes []string
	peerPicker PeerPicker
	peerConnection map[string]*pool.ObjectPool

	db databaseInter.EmbedDB
	transactions *dict.SimpleDict   // id -> Transaction

	idGenerator *idgenerator.IDGenerator
	relayImpl func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply
}

func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:           config.Properties.Self,
		peerPicker:     consistenthash.New(replicas, nil),
		peerConnection: make(map[string]*pool.ObjectPool),
		db:             database.NewStandaloneServer(),
		transactions:   dict.MakeSimple(),
		idGenerator:    idgenerator.MakeGenerator(config.Properties.Self),
		relayImpl:      defaultRelayImpl,
	}

	contains := make(map[string]struct{})
	nodes := make([]string, 0, len(config.Properties.Peers) + 1)
	for _, peer := range config.Properties.Peers {
		if _, ok := contains[peer]; ok {
			continue
		}

		contains[peer] = struct{}{}
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)

	cluster.peerPicker.AddNode(nodes...)

	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{Peer: peer})
	}

	cluster.nodes = nodes
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = makeRouter()

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("error occurs: %v\n%s", err, string(debug.Stack()))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	// 一些特殊的命令，使用特殊的处理方法
	switch cmdName {
	case "multi":
		return
	case "discard":
		return
	case "exec":
		return
	case "select":
		return
	default:
	}

	// 常规的命令，则统一使用router中注册的方法处理
	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}

	result = cmdFunc(cluster, c, cmdLine)
	return
}

func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}