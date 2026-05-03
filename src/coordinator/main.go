package main

import (
	"context"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	pubsubClient, topic, subscription := setupPubSub(ctx)
	if pubsubClient == nil {
		log.Fatalf("Failed to setup pubsub")
	}
	defer pubsubClient.Close()

	// Simulator uses csv as source
	simulator := NewSimulator(topic, "../../data/ais.csv")
	go simulator.Run(ctx)

	windowSizeInSeconds := 60
	coordinator := NewCoordinator(redisClient, windowSizeInSeconds)
	coordinator.Run(ctx, subscription)
}
