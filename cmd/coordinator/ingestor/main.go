package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/mardentub/coordinator/config"
	"github.com/redis/go-redis/v9"
)

const (
	dataKey    = "data"
	sessionKey = "sessionKey"
)

var (
	rdb       *redis.Client
	appConfig config.Config
)

func init() {

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatalln("REDIS_URL is empty")
	}

	appConfig = config.LoadConfig()

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

	sourceName, ok := fields["_source"].(string)
	if !ok || sourceName == "" {
		log.Printf("dropping event: missing or invalid '_source' field in event data")
		return nil
	}

	source, exists := appConfig.Sources[sourceName]
	if !exists {
		log.Printf("dropping event: unknown source %q in configuration", sourceName)
		return nil
	}

	timestampField := source.TimestampField
	timestampLayout := source.TimestampFormat
	idField := source.IDField

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

	id := fmt.Sprintf("%v", fields[idField])
	tSec := t.Unix()
	tStr := strconv.FormatInt(tSec, 10)
	if err := rdb.ZAdd(ctx, sessionKey+":"+id, redis.Z{
		Score:  float64(tSec),
		Member: tStr,
	}).Err(); err != nil {
		return fmt.Errorf("redis zadd session-time failed: %w", err)
	}

	return nil
}
