package subscriber

import (
	"context"
	"time"

	"github.com/aldadelas/redis-stream-go/client"
	"github.com/redis/go-redis/v9"
)

type SubscriberService struct {
	redisClient *client.RedisClient
}

func NewSubscriberService(redisClient *client.RedisClient) *SubscriberService {
	return &SubscriberService{redisClient: redisClient}
}

func (s *SubscriberService) Subscribe(ctx context.Context, streamName string, groupName string, consumerName string) ([]redis.XStream, error) {
	streams, err := s.redisClient.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    2,
		Block:    900 * time.Second,
	}).Result()

	if err != nil {
		return nil, err
	}

	return streams, nil
}

func (s *SubscriberService) Ack(ctx context.Context, streamName string, groupName string, consumerName string, messageID string) error {
	return s.redisClient.Client.XAck(ctx, streamName, groupName, messageID).Err()
}
