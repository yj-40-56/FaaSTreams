package main

import (
	"context"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddr := redisHost + ":" + redisPort

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
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
	simulator := NewSimulator(topic, "/app/data/ais.csv")
	go simulator.Run(ctx)

	windowSizeInSeconds := 60
	coordinator := NewCoordinator(redisClient, windowSizeInSeconds)
	coordinator.Run(ctx, subscription)
}
