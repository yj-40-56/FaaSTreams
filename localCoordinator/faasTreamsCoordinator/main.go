package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	payload := os.Getenv("PUBSUB_PAYLOAD")
	shipID := os.Getenv("SHIP_ID")

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	key := fmt.Sprintf("buffer:ship:%s", shipID)
	err := rdb.LPush(ctx, key, payload).Err()
	if err != nil {
		fmt.Printf("Redis Error: %v\n", err)
		return
	}

	fmt.Printf("Successfully pushed data for ship %s\n", shipID)
}
