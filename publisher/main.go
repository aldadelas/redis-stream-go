package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aldadelas/redis-stream-go/client"
	publisher "github.com/aldadelas/redis-stream-go/publisher/service"
)

func main() {
	redisClient, err := client.NewRedisClient(context.Background(), "localhost:6379")
	if err != nil {
		fmt.Println("Error creating Redis client:", err)
		return
	}

	publisherService := publisher.NewPublisherService(redisClient)

	for i := 0; i < 10; i++ {
		id, err := publisherService.Publish(context.Background(), "order-stream", map[string]interface{}{
			"message":   fmt.Sprintf("%d", i+1),
			"timestamp": time.Now().Unix(),
		})
		if err != nil {
			fmt.Println("Error publishing message:", err)
			return
		}

		fmt.Println("Published message:", id, "at", time.Now().Format(time.RFC3339))
	}
}
