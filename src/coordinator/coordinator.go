package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"
	"os"
	"bytes"
	"net/http"
	"io"

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
func (c *Coordinator) Run(ctx context.Context, sub *pubsub.Subscription) {
    // 1. Start a goroutine to keep consuming Pub/Sub messages and saving to Redis
    go func() {
        err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
            c.saveToRedis(ctx, msg.Data) // Your logic to parse JSON and ZADD to Redis
            msg.Ack()
        })
        if err != nil {
            log.Printf("[Coordinator] Pub/Sub receive error: %v", err)
        }
    }()

    // 2. Start the "Metronome" for calculations
    ticker := time.NewTicker(10 * time.Second) // Trigger every 10 simulated seconds
    defer ticker.Stop()

    // Start time for the first window
    windowStart := time.Unix(1777161600, 0) 

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            windowEnd := windowStart.Add(60 * time.Second)
            
            log.Printf("[Coordinator] Triggering calculation for window: %v to %v", windowStart, windowEnd)
            
            // Call the Worker
            go c.triggerWorker(ctx, windowStart, windowEnd)
            
            // Slide the window forward
            windowStart = windowStart.Add(10 * time.Second) 
        }
    }
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

// TODO: Pass window_start, window_end, query
func (c *Coordinator) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)

	log.Printf("[Coordinator] Triggering worker for window (scores): %s - %s\n", minScore, maxScore)

	for i := 0; i < len(c.queryConfig.SQLQueries); i++ {
		query := c.queryConfig.SQLQueries[i]
		log.Printf("[Coordnator] Triggering worker for query: %s\n", query.Name)
		//TODO: spawn worker, WIP I DONT KNOW HOW THIS WORKS YET
		workerURL := os.Getenv("WORKER_URL")
		if workerURL == "" {
			workerURL = "http://worker:8080"
		}

		body, err := json.Marshal(
			struct {
				StartTimestamp int64  `json:"start_timestamp"`
				EndTimestamp   int64  `json:"end_timestamp"`
				QueryName      string `json:"query_name"`
			}{
				StartTimestamp: windowStart.Unix(),
				EndTimestamp:   windowEnd.Unix(),
				QueryName:      c.queryConfig.SQLQueries[0].Name,
			})
		if err != nil {
			log.Printf("Marshal error: %v", err)
			return
		}
		resp, err := http.Post(workerURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			log.Printf("Error: %v", err)
			return
		}
		resBody, _ := io.ReadAll(resp.Body)
		log.Printf("[Coordinator] Worker response: %s", resp.Status, string(resBody))
	}
}
