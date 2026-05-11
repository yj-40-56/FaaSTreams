package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"coordinator/config"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

const windowSize = 60 * time.Second

type Event struct {
	Timestamp time.Time
}

// TODO: Currently only supports tumbling windows, add support for other window types
type Coordinator struct {
	redisClient *redis.Client
	queryConfig config.QueryConfig
}

func NewCoordinator(redisClient *redis.Client, queryConfig config.QueryConfig) *Coordinator {
	return &Coordinator{
		redisClient: redisClient,
		queryConfig: queryConfig,
	}
}

// Run is invoked by GCP when a Pub/Sub message arrives
func (c *Coordinator) Run(ctx context.Context, msg *pubsub.Message) error {
	log.Printf("[Coordinator] Received event")
	var raw map[string]string
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
		msg.Ack()
		return nil
	}
	timestamp, err := time.Parse("02/01/2006 15:04:05", raw["# Timestamp"])
	if err != nil {
		msg.Ack()
		return nil
	}
	event := &Event{Timestamp: timestamp}
	if err := c.processIncomingEvent(ctx, event, msg.Data); err != nil {
		return err
	}
	msg.Ack()
	return nil
}

// Store event in Redis sorted set and evaluate window boundary
func (c *Coordinator) processIncomingEvent(ctx context.Context, event *Event, rawData []byte) error {
	errRedis := c.redisClient.ZAdd(ctx, "mod-stream", redis.Z{
		Score:  float64(event.Timestamp.Unix()),
		Member: string(rawData),
	},
	).Err()

	if errRedis != nil {
		return errRedis
	}

	timeLeft, _ := c.redisClient.TTL(ctx, "window:active").Result()

	if timeLeft == -2 {
		c.redisClient.Set(ctx, "window:active", "1", 60*time.Second)
	} else if timeLeft > 0 {

	} else {
		from := int64(0)
		to, _ := c.redisClient.ZCard(ctx, "mod-stream").Result()
		to = to - 1

		for i := 0; i < len(c.queryConfig.SQLQueries); i++ {
			query := c.queryConfig.SQLQueries[i]
			c.triggerWorker(ctx, from, to, query)
		}

		c.redisClient.Set(ctx, "window:active", "1", 60*time.Second)
	}
	return nil
}

// TODO: Start actual worker, currently only logs
// TODO: Pass window_start, window_end, query
func (c *Coordinator) triggerWorker(ctx context.Context, from int64, to int64, query config.SQLQuery) {
	log.Printf("[Coordinator] Triggering worker for query: %s, from: %d, to: %d\n", query.Name, from, to)
}
