package ingestorcore

import (
	"context"
	"log"
	"os"

	"github.com/faastreams/ingestor/config"

	"github.com/redis/go-redis/v9"
)

// SetupFromEnv builds an Ingestor wired to Redis and the source config, all configured via env vars.
func SetupFromEnv(ctx context.Context) *Ingestor {
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

	cfg := config.LoadConfig()
	return NewIngestor(redisClient, cfg.Sources)
}
