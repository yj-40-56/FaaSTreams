package main

import (
	"context"
	"coordinator/config"
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

	pubsubClient, _, subscription := setupPubSub(ctx)
	if pubsubClient == nil {
		log.Fatalf("Failed to setup pubsub")
	}
	defer pubsubClient.Close()

	configPath := os.Getenv("CONFIG_PATH")
	queryConfig := config.LoadConfig(configPath)
	// TODO: For testing purposes we just select the first query config add support for several later
	selectedQuery := queryConfig.Queries[0]

	coordinator := NewCoordinator(redisClient, selectedQuery)
	coordinator.Run(ctx, subscription)
}
