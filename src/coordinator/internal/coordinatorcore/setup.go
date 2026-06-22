package coordinatorcore

import (
	"context"
	"log"
	"os"

	"github.com/faastreams/coordinator/query"

	"github.com/redis/go-redis/v9"
)

// SetupFromEnv builds a Coordinator wired to Redis and the first query config, all configured via env vars.
func SetupFromEnv(ctx context.Context) []*Coordinator {
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

	queryConfig := query.LoadConfig()
	// TODO: For testing purposes we just select the first query config add support for several later
	// selectedQuery := queryConfig.Queries[0]
	coordinators := make([]*Coordinator, 0, len(queryConfig.Queries))
	for _, q := range queryConfig.Queries {
		coordinators = append(coordinators, NewCoordinator(redisClient, q))
	}
	return coordinators
}
