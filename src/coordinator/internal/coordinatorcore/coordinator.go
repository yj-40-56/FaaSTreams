package coordinatorcore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/faastreams/coordinator/config"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

var redisStreamKey = getEnvDefault("REDIS_KEY", "mod-stream")
var coordinatorKeyPrefix = getEnvDefault("COORDINATOR_KEY_PREFIX", "coordinator")

func getEnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type Event struct {
	Timestamp time.Time
	Raw       map[string]string
}

// TODO: Currently only supports tumbling windows, add support for other window types
// Coordinator listens to Pub/Sub messages, extracts event data and stores it in Redis sorted set
type Coordinator struct {
	redisClient *redis.Client
	windowSize  time.Duration
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

func (c *Coordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse Pub/Sub requesst
	var pushRequest struct {
		Message struct {
			Data []byte `json:"data"`
		} `json:"message"`
	}

	json.NewDecoder(r.Body).Decode(&pushRequest)

	var data map[string]string
	json.Unmarshal(pushRequest.Message.Data, &data)

	event := c.parseEventFromMap(data)
	c.handleEvent(r.Context(), event, pushRequest.Message.Data)

	w.WriteHeader(http.StatusOK)
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

// Retrieve current windowEnd from Redis, if not set return zero time
func (c *Coordinator) getWindowEnd(ctx context.Context) time.Time {
	val, err := c.redisClient.Get(ctx, coordinatorKeyPrefix+":window_end").Result()
	if err == redis.Nil {
		log.Println("[Coordinator] window_end key missing in Redis, treating as first window")
		return time.Time{}
	}
	if err != nil {
		log.Printf("[Coordinator] ERROR reading window_end from Redis: %v\n", err)
		return time.Time{}
	}
	unix, _ := strconv.ParseInt(val, 10, 64)
	return time.Unix(unix, 0)
}

func (c *Coordinator) setWindowEnd(ctx context.Context, t time.Time) {
	c.redisClient.Set(ctx, coordinatorKeyPrefix+":window_end", t.Unix(), 0)
}

// Store event in Redis sorted set, where score is timestamp and member is JSON data, sorted by score
func (c *Coordinator) handleEvent(ctx context.Context, event *Event, rawData []byte) {
	windowEnd := c.getWindowEnd(ctx)
	if windowEnd.IsZero() {
		windowEnd = event.Timestamp.Add(c.windowSize)
		c.setWindowEnd(ctx, windowEnd)
		log.Printf("[Coordinator] First window ends at: %s\n", windowEnd.Format("15:04:05"))
	}

	score := float64(event.Timestamp.Unix())

	// Store raw JSON directly in Redis
	c.redisClient.ZAdd(ctx, redisStreamKey, redis.Z{
		Score:  score,
		Member: string(rawData),
	})

	// Start worker for previous window
	if event.Timestamp.After(windowEnd) {
		windowStart := windowEnd.Add(-c.windowSize)
		c.triggerWorker(ctx, windowStart, windowEnd)
		windowEnd = windowEnd.Add(c.windowSize)
		c.setWindowEnd(ctx, windowEnd)
	}
}

// TODO: Pass window_start, window_end, query
func (c *Coordinator) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	lockKey := fmt.Sprintf("%s:lock:%d", coordinatorKeyPrefix, windowEnd.Unix())

	// One worker instace per window
	locked, err := c.redisClient.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil || !locked {
		log.Printf("[Coordinator] Window %s already being processed, skipping\n", windowEnd.Format("15:04:05"))
		return
	}

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
			"query_name": query.Name,
			"return_type": query.ReturnType,
		}

		dataBytes, _ := json.Marshal(data)

		go func() {
			resp, err := http.Post(workerURL, "application/json", bytes.NewBuffer(dataBytes))
			if err != nil {
				log.Printf("[Coordinator] Failed to spawn worker for query %s: %v\n", query.Name, err)
				return
			}
			defer resp.Body.Close()
			log.Printf("[Coordinator] Worker spawned for query: %s\n", query.Name)
		}()
	}
}
