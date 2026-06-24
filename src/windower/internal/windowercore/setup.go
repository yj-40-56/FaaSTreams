package windowercore

import (
	"context"
	"log"
	"os"

	"github.com/faastreams/windower/config"

	"github.com/redis/go-redis/v9"
)

// SetupFromEnv builds a Manager wired to Redis, with one Windower per configured query, all
// configured via env vars.
func SetupFromEnv(ctx context.Context) *Manager {
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
		if err := ValidateQuery(q); err != nil {
			log.Printf("[Windower] Skipping query %q: %v", q.Name, err)
			continue
		}
		windowers = append(windowers, NewWindower(redisClient, q, queryConfig.Sources))
	}
	return NewManager(redisClient, windowers)
}
