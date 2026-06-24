package ingestorcore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/faastreams/ingestor/config"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

var redisStreamKey = getEnvDefault("REDIS_KEY", "mod-stream")
var sessionKeyPrefix = getEnvDefault("SESSION_KEY_PREFIX", "session")

func getEnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type Event struct {
	Timestamp  time.Time
	Raw        map[string]string
	SourceName string
	ID         string
}

// Ingestor listens to Pub/Sub messages, extracts event data and stores it in a Redis sorted set,
// scored by event timestamp, for downstream windowing.
type Ingestor struct {
	redisClient *redis.Client
	sources     map[string]config.Source
}

func NewIngestor(redisClient *redis.Client, sources map[string]config.Source) *Ingestor {
	return &Ingestor{
		redisClient: redisClient,
		sources:     sources,
	}
}

// Run listens to Pub/Sub messages, extracts event data and stores it in Redis sorted set
func (i *Ingestor) Run(ctx context.Context, subscription *pubsub.Subscription) {
	subscription.ReceiveSettings.MaxOutstandingMessages = 1
	subscription.ReceiveSettings.NumGoroutines = 1
	subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var data map[string]string
		err := json.Unmarshal(msg.Data, &data)
		if err != nil {
			msg.Ack()
			return
		}

		event := i.parseEventFromMap(data)
		if event != nil {
			i.storeEvent(ctx, event, msg.Data)
		}

		msg.Ack()
	})
}

func (i *Ingestor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	i.HandleMessage(r.Context(), body)
	w.WriteHeader(http.StatusOK)
}

func (i *Ingestor) HandleMessage(ctx context.Context, body []byte) {
	var pushRequest struct {
		Message struct {
			Data []byte `json:"data"`
		} `json:"message"`
	}
	json.Unmarshal(body, &pushRequest)

	var data map[string]string
	json.Unmarshal(pushRequest.Message.Data, &data)

	event := i.parseEventFromMap(data)
	if event == nil {
		return
	}
	i.storeEvent(ctx, event, pushRequest.Message.Data)
}

// Write sub data into Event struct
func (i *Ingestor) parseEventFromMap(data map[string]string) *Event {
	sourceName := data["_source"]
	source, exists := i.sources[sourceName]

	if !exists {
		log.Printf("[Ingestor] Unknown source: %s\n", sourceName)
		return nil
	}

	timestamp, err := time.Parse(source.TimestampFormat, data[source.TimestampField])
	if err != nil {
		return nil
	}

	return &Event{
		Timestamp:  timestamp,
		Raw:        data,
		SourceName: sourceName,
		ID:         data[source.IDField],
	}
}

// Store event in Redis sorted set, where score is timestamp and member is JSON data, sorted by score.
// Also records the event's timestamp under a per-ID sorted set so the windower can derive session
// windows (gaps between consecutive events for the same ID) without re-parsing the raw stream.
func (i *Ingestor) storeEvent(ctx context.Context, event *Event, rawData []byte) {
	score := float64(event.Timestamp.Unix())
	i.redisClient.ZAdd(ctx, redisStreamKey, redis.Z{
		Score:  score,
		Member: string(rawData),
	})

	if event.ID != "" {
		i.redisClient.ZAdd(ctx, fmt.Sprintf("%s:%s", sessionKeyPrefix, event.ID), redis.Z{
			Score:  score,
			Member: strconv.FormatInt(event.Timestamp.Unix(), 10),
		})
	}
}
