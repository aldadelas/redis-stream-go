package client

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

func NewRedisClient(ctx context.Context, addr string) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return &RedisClient{Client: client}, nil
}

func (c *RedisClient) CreateStreamGroup(ctx context.Context, streamName string, groupName string) error {
	groups, err := c.Client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		if err.Error() != "ERR no such key" {
			return err
		}
	}

	for _, group := range groups {
		if group.Name == groupName {
			return nil
		}
	}

	err = c.Client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil {
		return err
	}

	return nil
}
