package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

type Event struct {
	Timestamp time.Time
	Raw       map[string]string
}

// TODO: Currently only supports tumbling windows, later infos like windowSize should be set by Query Config
// Coordinator listens to Pub/Sub messages, extracts event data and stores it in Redis sorted set
type Coordinator struct {
	redisClient *redis.Client
	windowSize  time.Duration
	windowEnd   time.Time
	initialized bool
}

func NewCoordinator(redisClient *redis.Client, windowSizeInSeconds int) *Coordinator {
	windowSize := time.Duration(windowSizeInSeconds) * time.Second

	coordinator := &Coordinator{
		redisClient: redisClient,
		windowSize:  windowSize,
		initialized: false,
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
	if c.initialized == false {
		c.windowEnd = event.Timestamp.Add(c.windowSize)
		c.initialized = true
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

// TODO: Pass this logic to worker, Coordinator should just trigger the worker
func (c *Coordinator) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)

	// Retrieve results for the window
	results, _ := c.redisClient.ZRangeByScore(ctx, "mod-stream", &redis.ZRangeBy{
		Min: minScore,
		Max: maxScore,
	}).Result()

	log.Printf("[Coordinator] Window from %s to %s closed with %d events\n",
		windowStart.Format("15:04:05"),
		windowEnd.Format("15:04:05"),
		len(results))

	// Window data would be deleted from Redis, when worker finishes
	//c.redisClient.ZRemRangeByScore(ctx, "mod-stream", minScore, maxScore)
	//log.Printf("Window data deleted from Redis")
}
