package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/redis/go-redis/v9"
)

const dataKey = "data"

var (
	rdb             *redis.Client
	timestampField  string
	timestampLayout string
)

func init() {

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatalln("REDIS_URL is empty")
	}

	timestampField = os.Getenv("TIMESTAMP_FIELD")
	if timestampField == "" {
		log.Fatalln("TIMESTAMP_FIELD is empty")
	}
	timestampLayout = os.Getenv("TIMESTAMP_LAYOUT")
	if timestampLayout == "" {
		log.Fatalln("TIMESTAMP_LAYOUT is empty")
	}

	rdb = redis.NewClient(&redis.Options{
		Addr:     redisURL,
		PoolSize: 50,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis not reachable at %s: %v", redisURL, err)
	}

	functions.CloudEvent("IngestEvent", ingestEvent)
}

func ingestEvent(ctx context.Context, e event.Event) error {
	var pubSubMessage struct {
		Message struct {
			Data []byte `json:"data"`
		} `json:"message"`
	}
	if err := json.Unmarshal(e.Data(), &pubSubMessage); err != nil {
		return fmt.Errorf("pubsub envelope parse error: %w", err)
	}

	var fields map[string]interface{}
	if err := json.Unmarshal(pubSubMessage.Message.Data, &fields); err != nil {
		return fmt.Errorf("event schema error: %w", err)
	}

	tsRaw, ok := fields[timestampField].(string)
	if !ok {
		log.Printf("dropping event: missing/non-string timestamp field %q", timestampField)
		return nil
	}

	t, err := time.Parse(timestampLayout, tsRaw)
	if err != nil {
		log.Printf("timestamp format error: %v", err)
		return nil
	}

	if err := rdb.ZAdd(ctx, dataKey, redis.Z{
		Score:  float64(t.Unix()),
		Member: string(pubSubMessage.Message.Data),
	}).Err(); err != nil {
		return fmt.Errorf("redis zadd failed: %w", err)
	}

	return nil
}
