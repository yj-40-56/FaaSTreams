package windowercore

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

	"github.com/faastreams/windower/config"

	"github.com/redis/go-redis/v9"
)

var redisStreamKey = getEnvDefault("REDIS_KEY", "mod-stream")
var windowerKeyPrefix = getEnvDefault("WINDOWER_KEY_PREFIX", "windower")

func getEnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// TODO: Currently only supports tumbling windows, add support for other window types
// Windower is ticked periodically (by Cloud Scheduler) and advances the query's window based on
// the latest event timestamp already stored in Redis by the ingestor, triggering a worker for
// each window that has closed since the last tick.
type Windower struct {
	redisClient  *redis.Client
	windowSize   time.Duration
	query        config.Query
	windowEndKey string
	source       config.Source
}

func NewWindower(redisClient *redis.Client, queryConfig config.Query, sources map[string]config.Source) *Windower {
	windowSize := time.Duration(queryConfig.WindowSize) * time.Second

	return &Windower{
		redisClient:  redisClient,
		windowSize:   windowSize,
		query:        queryConfig,
		windowEndKey: fmt.Sprintf("%s:%s:window_end", windowerKeyPrefix, queryConfig.Name),
		source:       sources[queryConfig.DataSource],
	}
}

func (w *Windower) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.Tick(r.Context())
	rw.WriteHeader(http.StatusOK)
}

// Tick checks the latest ingested event timestamp against the current window_end and triggers
// a worker for every window that has closed since the previous tick.
func (w *Windower) Tick(ctx context.Context) {
	latest, ok := w.latestEventTimestamp(ctx)
	if !ok {
		return
	}

	windowEnd := w.getWindowEnd(ctx)
	if windowEnd.IsZero() {
		windowEnd = latest.Add(w.windowSize)
		w.setWindowEnd(ctx, windowEnd)
		log.Printf("[Windower:%s] First window ends at: %s\n", w.query.Name, windowEnd.Format("15:04:05"))
		return
	}

	for latest.After(windowEnd) {
		windowStart := windowEnd.Add(-w.windowSize)
		w.triggerWorker(ctx, windowStart, windowEnd)

		cleanupUpperBound := windowEnd.Add(-2 * w.windowSize)
		w.redisClient.ZRemRangeByScore(ctx, redisStreamKey, "-inf", strconv.FormatInt(cleanupUpperBound.Unix(), 10))

		windowEnd = windowEnd.Add(w.windowSize)
		w.setWindowEnd(ctx, windowEnd)
	}
}

// latestEventTimestamp returns the timestamp of the most recently ingested event, if any.
func (w *Windower) latestEventTimestamp(ctx context.Context) (time.Time, bool) {
	result, err := w.redisClient.ZRevRangeWithScores(ctx, redisStreamKey, 0, 0).Result()
	if err != nil {
		log.Printf("[Windower] ERROR reading latest event from Redis: %v\n", err)
		return time.Time{}, false
	}
	if len(result) == 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(result[0].Score), 0), true
}

// Retrieve current windowEnd from Redis, if not set return zero time
func (w *Windower) getWindowEnd(ctx context.Context) time.Time {
	val, err := w.redisClient.Get(ctx, w.windowEndKey).Result()
	if err == redis.Nil {
		log.Println("[Windower] window_end key missing in Redis, treating as first window")
		return time.Time{}
	}
	if err != nil {
		log.Printf("[Windower] ERROR reading window_end from Redis: %v\n", err)
		return time.Time{}
	}
	unix, _ := strconv.ParseInt(val, 10, 64)
	return time.Unix(unix, 0)
}

func (w *Windower) setWindowEnd(ctx context.Context, t time.Time) {
	w.redisClient.Set(ctx, w.windowEndKey, t.Unix(), 0)
}

// TODO: Pass window_start, window_end, query
func (w *Windower) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	lockKey := fmt.Sprintf("%s:%s:lock:%d", windowerKeyPrefix, w.query.Name, windowEnd.Unix())

	// One worker instance per window
	locked, err := w.redisClient.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil || !locked {
		log.Printf("[Windower] Window %s already being processed, skipping\n", windowEnd.Format("15:04:05"))
		return
	}

	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)

	log.Printf("[Windower] Triggering worker for window (scores): %s(%s) - %s(%s)\n", minScore, windowStart.Format("15:04:05"), maxScore, windowEnd.Format("15:04:05"))
	workerURL := os.Getenv("WORKER_URL")

	data := map[string]interface{}{
		"window_start":     windowStart.Unix(),
		"window_end":       windowEnd.Unix(),
		"query_name":       w.query.Name,
		"query":            w.query.Query,
		"return_type":      w.query.ReturnType,
		"is_alert":         w.query.IsAlert,
		"alert_format":     w.query.AlertFormat,
		"data_source":      w.query.DataSource,
		"columns":          w.source.Columns,
		"reference_tables": w.source.ReferenceTables,
	}

	dataBytes, _ := json.Marshal(data)

	go func() {
		resp, err := http.Post(workerURL, "application/json", bytes.NewBuffer(dataBytes))
		if err != nil {
			log.Printf("[Windower:%s] Failed to spawn worker: %v\n", w.query.Name, err)
			return
		}
		defer resp.Body.Close()
		log.Printf("[Windower:%s] Worker spawned\n", w.query.Name)
	}()
}
