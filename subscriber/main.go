package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aldadelas/redis-stream-go/client"
	subscriber "github.com/aldadelas/redis-stream-go/subscriber/service"
)

func main() {
	// same stream name
	// different group name
	var groupName string
	fmt.Println("Enter group name:")
	fmt.Scanln(&groupName)
	// different consumer name
	var consumerName string
	fmt.Println("Enter consumer name:")
	fmt.Scanln(&consumerName)

	redisClient, err := client.NewRedisClient(context.Background(), "localhost:6379")
	redisClient.CreateStreamGroup(context.Background(), "order-stream", groupName)
	if err != nil {
		fmt.Println("Error creating Redis client:", err)
		return
	}

	subscriberService := subscriber.NewSubscriberService(redisClient)
	ctx, _ := context.WithCancel(context.Background())

	// timer := time.AfterFunc(60*time.Second, func() {
	// 	cancel()
	// })
	// defer timer.Stop()

	totalMessages := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		streams, err := subscriberService.Subscribe(ctx, "order-stream", groupName, consumerName)
		if err != nil {
			fmt.Println("Error subscribing to stream:", err)
			return
		}

		for _, stream := range streams {
			totalMessages += len(stream.Messages)
			for _, message := range stream.Messages {
				num, err := strconv.Atoi(message.Values["message"].(string))
				if err != nil {
					fmt.Println("Error converting message to number:", err)
					// subscriberService.Ack(ctx, "order-stream", groupName, consumerName, message.ID)
					continue
				}

				if groupName == "odd-number" && num%2 != 0 {
					fmt.Printf("Received odd number: %d\n", num)
				} else if groupName == "even-number" && num%2 == 0 {
					fmt.Printf("Received even number: %d\n", num)
				} else {
					fmt.Printf("message is not %s: %d\n", groupName, num)
				}
				subscriberService.Ack(ctx, "order-stream", groupName, consumerName, message.ID)
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
