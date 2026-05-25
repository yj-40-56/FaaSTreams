package myfunction

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

type PubSubMessage struct {
	Message struct {
		Data []byte `json:"data"`
	} `json:"message"`
}

type AISData struct {
	ID        string  `json:"id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp string  `json:"timestamp"`
}

func init() {
	functions.CloudEvent("ProcessAisData", processAisData)
}

func processAisData(ctx context.Context, e event.Event) error {
	log.Printf("DEBUG: Rohdaten vom Event: %s", string(e.Data()))
	redisURL := os.Getenv("REDIS_URL")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisURL,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return err
	}

	var msg PubSubMessage

	if err := json.Unmarshal(e.Data(), &msg); err != nil {
		return fmt.Errorf("pubsub envelope parse error: %w", err)
	}

	var ais AISData

	if err := json.Unmarshal(msg.Message.Data, &ais); err != nil {
		return fmt.Errorf("ais parse error: %w", err)
	}

	log.Printf("Parsed vessel: %+v", ais)

	t, err := time.Parse(time.RFC3339, ais.Timestamp)
	if err != nil {
		return fmt.Errorf("timestamp format error: %w", err)
	}

	unixTime := t.Unix()
	windowSize := int64(30)
	windowStart := (unixTime / windowSize) * windowSize
	key := fmt.Sprintf("ais:bucket:%d", windowStart)

	data, err := json.Marshal(ais)
	if err != nil {
		return fmt.Errorf("error marshaling ais data: %w", err)
	}

	err = rdb.RPush(ctx, key, data).Err()
	if err != nil {
		return fmt.Errorf("redis RPush failed: %w", err)
	}

	rdb.Expire(ctx, key, 5*time.Minute)
	log.Printf("Successfully added vessel %s to bucket %d", ais.ID, windowStart)
	return nil
}
