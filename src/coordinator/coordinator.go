package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"coordinator/config"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

type Event struct {
	Timestamp time.Time
	Raw       map[string]string
}

// TODO: Currently only supports tumbling windows, add support for other window types
// Coordinator listens to Pub/Sub messages, extracts event data and stores it in Redis sorted set
type Coordinator struct {
	redisClient *redis.Client
	windowSize  time.Duration
	windowEnd   time.Time
	queryConfig config.QueryConfig
}

func NewCoordinator(redisClient *redis.Client, queryConfig config.QueryConfig) *Coordinator {
	windowSize := time.Duration(queryConfig.WindowSizeInSeconds) * time.Second

	coordinator := &Coordinator{
		redisClient: redisClient,
		windowSize:  windowSize,
		queryConfig: queryConfig,
	}

	return coordinator
}

// Run listens to Pub/Sub messages, extracts event data and stores it in Redis sorted set
func (c *Coordinator) Run(ctx context.Context, subscription *pubsub.Subscription) {
	subscription.ReceiveSettings.MaxOutstandingMessages = 1
	subscription.ReceiveSettings.NumGoroutines = 1
	subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var data map[string]string
		err := json.Unmarshal(msg.Data, &data)
		if err != nil {
			msg.Ack()
			return
		}

		event := c.parseEventFromMap(data)
		c.handleEvent(ctx, event, msg.Data)

		msg.Ack()
	})
}

// Write sub data into Event struct
func (c *Coordinator) parseEventFromMap(data map[string]string) *Event {
	timestamp, err := time.Parse("02/01/2006 15:04:05", data["# Timestamp"])
	if err != nil {
		return nil
	}

	return &Event{
		Timestamp: timestamp,
		Raw:       data,
	}
}

// Store event in Redis sorted set, where score is timestamp and member is JSON data, sorted by score
func (c *Coordinator) handleEvent(ctx context.Context, event *Event, rawData []byte) {
	if c.windowEnd.IsZero() {
		c.windowEnd = event.Timestamp.Add(c.windowSize)
		log.Printf("[Coordinator] First window ends at: %s\n", c.windowEnd.Format("15:04:05"))
	}

	score := float64(event.Timestamp.Unix())

	// Store raw JSON directly in Redis
	c.redisClient.ZAdd(ctx, "mod-stream", redis.Z{
		Score:  score,
		Member: string(rawData),
	})

	// Start worker for previous window
	if event.Timestamp.After(c.windowEnd) {
		windowStart := c.windowEnd.Add(-c.windowSize)
		c.triggerWorker(ctx, windowStart, c.windowEnd)
		c.windowEnd = c.windowEnd.Add(c.windowSize)
	}
}

func (c *Coordinator) removeWindowData(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)
	c.redisClient.ZRemRangeByScore(ctx, "mod-stream", minScore, maxScore)
}

// TODO: Pass window_start, window_end, query
func (c *Coordinator) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)

	log.Printf("[Coordinator] Triggering worker for window (scores): %s - %s\n", minScore, maxScore)
	workerURL := os.Getenv("WORKER_URL")

	for i := 0; i < len(c.queryConfig.SQLQueries); i++ {
		query := c.queryConfig.SQLQueries[i]
		data := map[string]interface{}{
			"window_start": windowStart.Unix(),
			"window_end":   windowEnd.Unix(),
			"query":        query.Query,
		}

		dataBytes, _ := json.Marshal(data)

		go func() {
			resp, err := http.Post(workerURL, "application/json", bytes.NewBuffer(dataBytes))
			if err != nil {
				log.Printf("[Coordinator] Failed to spawn worker for query %s: %v \n", query.Name, err)
				return
			}
			defer resp.Body.Close()
			log.Printf("[Coordinator] Worker spawned for query: %s\n", query.Name)
		}()
	}
}
