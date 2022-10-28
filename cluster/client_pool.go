package cluster

import (
	"context"
	"errors"
	"github.com/jolestar/go-commons-pool/v2"
	"studygolang/wangdis/config"
	"studygolang/wangdis/lib/utils"
	"studygolang/wangdis/redis/client"
)

type connectionFactory struct {
	Peer string
}

func (f *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()

	if config.Properties.RequirePass != "" {
		c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
	}
	return pool.NewPooledObject(c), nil
}

func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (f *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	panic("implement me")
}

func (f *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	panic("implement me")
}

func (f *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	panic("implement me")
}
