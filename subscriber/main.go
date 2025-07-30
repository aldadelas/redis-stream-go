package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aldadelas/redis-stream-go/client"
	subscriber "github.com/aldadelas/redis-stream-go/subscriber/service"
)

func main() {
	redisClient, err := client.NewRedisClient(context.Background(), "localhost:6379")
	redisClient.CreateStreamGroup(context.Background(), "test", "test-group")
	if err != nil {
		fmt.Println("Error creating Redis client:", err)
		return
	}

	subscriberService := subscriber.NewSubscriberService(redisClient)
	ctx, cancel := context.WithCancel(context.Background())

	timer := time.AfterFunc(60*time.Second, func() {
		cancel()
	})
	defer timer.Stop()

	totalMessages := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		streams, err := subscriberService.Subscribe(ctx, "test", "test-group", "test-consumer")
		if err != nil {
			fmt.Println("Error subscribing to stream:", err)
			return
		}

		for _, stream := range streams {
			totalMessages += len(stream.Messages)
			for _, message := range stream.Messages {
				fmt.Printf("Received message: %v\n", message)
				subscriberService.Ack(ctx, "test", "test-group", "test-consumer", message.ID)
			}

			// to prevent flooding request to redis
			if totalMessages >= 10 || totalMessages == 0 {
				time.Sleep(10 * time.Second)
				totalMessages = 0
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}
}
