package cluster

import (
	"context"
	"errors"
	"strconv"
	"studygolang/wangdis/interface/redis"
	"studygolang/wangdis/lib/utils"
	"studygolang/wangdis/redis/client"
	"studygolang/wangdis/redis/protocol"
)

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}

	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

func (cluster *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return factory.ReturnObject(context.Background(), peerClient)
}

// 默认的转发实现
var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
	// 一致性hash选出来的节点是当前节点，则直接在当前节点进行数据处理；否则就将命令转发到对应的节点上去进行处理
	if node == cluster.self {
		return cluster.db.Exec(c, cmdLine)
	}

	peerClient, err := cluster.getPeerClient(node)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(node, peerClient)
	}()

	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(cmdLine)
}

func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.relayImpl(cluster, peer, c, args)
}

// 对于某些特殊的命令，比如keys、pubsub，需要将命令广播给集群中所有的节点去执行
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}