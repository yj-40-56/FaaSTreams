package windowercore

import (
	"context"
	"log"
	"os"

	"github.com/faastreams/windower/config"

	"github.com/redis/go-redis/v9"
)

// SetupFromEnv builds Windowers wired to Redis, one per configured query, all configured via env vars.
func SetupFromEnv(ctx context.Context) []*Windower {
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

	queryConfig := config.LoadConfig()
	windowers := make([]*Windower, 0, len(queryConfig.Queries))
	for _, q := range queryConfig.Queries {
		windowers = append(windowers, NewWindower(redisClient, q, queryConfig.Sources))
	}
	return windowers
}
