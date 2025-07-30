package publisher

import (
	"context"

	"github.com/aldadelas/redis-stream-go/client"
	"github.com/redis/go-redis/v9"
)

type PublisherService struct {
	redisClient *client.RedisClient
}

func NewPublisherService(redisClient *client.RedisClient) *PublisherService {
	return &PublisherService{redisClient: redisClient}
}

func (s *PublisherService) Publish(ctx context.Context, streamName string, message map[string]interface{}) (string, error) {
	id, err := s.redisClient.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Approx: true,
		Values: message,
	}).Result()

	if err != nil {
		return "", err
	}

	return id, nil
}
