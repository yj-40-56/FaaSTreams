package windowercore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
var sessionKeyPrefix = getEnvDefault("SESSION_KEY_PREFIX", "session")

// windowNextKey is a single sorted set shared by every query: member is the query's progress
// key, score is that query's next window-start (in unix seconds). Sharing one set lets cleanup
// find the minimum in-progress window-start across all queries.
var windowNextKey = windowerKeyPrefix + ":window_next"

// sessionGapSeconds is the inactivity gap that closes a session window.
const sessionGapSeconds = 300

func getEnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// recordSetWindowStart atomically records a query's next window-start in the shared window_next set.
var recordSetWindowStart = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// cleanupBelowMin trims the raw event stream up to the earliest window-start still in play across
// all queries, so a slow/large-window query's data is never deleted out from under it by a faster one.
var cleanupBelowMin = redis.NewScript(`
local lo = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if lo[2] == nil then
    return 0
end
return redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. lo[2])
`)

// TODO: window types are tumbling, sliding and session; add support for hopping/calendar windows
// Windower is ticked periodically (by Cloud Scheduler) and advances the query's window based on
// the latest event timestamp already stored in Redis by the ingestor, triggering a worker for
// each window that has closed since the last tick.
type Windower struct {
	redisClient *redis.Client
	windowSize  time.Duration
	slideSize   time.Duration
	query       config.Query
	source      config.Source
}

// ValidateQuery rejects configs that would make handleTumbling/handleSliding loop forever
// (window_size <= 0, or a sliding window whose effective slide_size resolves to <= 0) or that
// are missing what handleSession needs.
func ValidateQuery(q config.Query) error {
	switch q.WindowType {
	case "session":
		if q.SessionID == "" {
			return fmt.Errorf("session window %q missing session_id", q.Name)
		}
	default: // "sliding" falls back to WindowSize when SlideSize is unset, so the same check covers it
		if q.WindowSize <= 0 {
			return fmt.Errorf("window %q has non-positive window_size (%d)", q.Name, q.WindowSize)
		}
	}
	return nil
}

func NewWindower(redisClient *redis.Client, queryConfig config.Query, sources map[string]config.Source) *Windower {
	slideSize := time.Duration(queryConfig.SlideSize) * time.Second
	if slideSize <= 0 {
		slideSize = time.Duration(queryConfig.WindowSize) * time.Second
	}

	return &Windower{
		redisClient: redisClient,
		windowSize:  time.Duration(queryConfig.WindowSize) * time.Second,
		slideSize:   slideSize,
		query:       queryConfig,
		source:      sources[queryConfig.DataSource],
	}
}

// Tick advances this query's window based on the latest ingested event timestamp, dispatching to
// the handler for the configured window type.
func (w *Windower) Tick(ctx context.Context) error {
	latest, ok := w.latestEventTimestamp(ctx)
	if !ok {
		return nil
	}

	switch w.query.WindowType {
	case "sliding":
		return w.handleSliding(ctx, latest)
	case "session":
		return w.handleSession(ctx, latest)
	default:
		return w.handleTumbling(ctx, latest)
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

func (w *Windower) lockKey() string {
	return fmt.Sprintf("%s:lock:%s", windowerKeyPrefix, w.query.Name)
}

func (w *Windower) setWindowStart(ctx context.Context, member string, startSec int64) error {
	return recordSetWindowStart.Run(ctx, w.redisClient, []string{windowNextKey}, strconv.FormatInt(startSec, 10), member).Err()
}

// handleTumbling advances a tumbling window: fixed-size, non-overlapping, one trigger per window.
func (w *Windower) handleTumbling(ctx context.Context, latest time.Time) error {
	windowSec := int64(w.windowSize.Seconds())
	if windowSec <= 0 {
		return fmt.Errorf("tumbling window %q has non-positive window_size (%ds)", w.query.Name, windowSec)
	}

	startScore, err := w.redisClient.ZScore(ctx, windowNextKey, w.query.Name).Result()
	if errors.Is(err, redis.Nil) {
		return w.setWindowStart(ctx, w.query.Name, latest.Unix())
	}
	if err != nil {
		return fmt.Errorf("redis zscore failed: %w", err)
	}

	endSec := int64(startScore) + windowSec
	tSec := latest.Unix()
	if tSec <= endSec {
		return nil
	}

	locked, err := w.redisClient.SetNX(ctx, w.lockKey(), "1", 2*time.Minute).Result()
	if err != nil || !locked {
		return nil
	}
	defer w.redisClient.Del(ctx, w.lockKey())

	for tSec > endSec {
		windowStart := endSec - windowSec
		w.triggerWorker(ctx, time.Unix(windowStart, 0).UTC(), time.Unix(endSec, 0).UTC())
		endSec += windowSec
	}

	return w.setWindowStart(ctx, w.query.Name, endSec-windowSec)
}

// handleSliding advances a sliding window: fixed-size, overlapping, stepping by SlideSize.
func (w *Windower) handleSliding(ctx context.Context, latest time.Time) error {
	windowSec := int64(w.windowSize.Seconds())
	slideSec := int64(w.slideSize.Seconds())
	if windowSec <= 0 || slideSec <= 0 {
		return fmt.Errorf("sliding window %q has non-positive window_size (%ds) or slide_size (%ds)", w.query.Name, windowSec, slideSec)
	}

	startScore, err := w.redisClient.ZScore(ctx, windowNextKey, w.query.Name).Result()
	if errors.Is(err, redis.Nil) {
		return w.setWindowStart(ctx, w.query.Name, latest.Unix())
	}
	if err != nil {
		return fmt.Errorf("redis zscore failed: %w", err)
	}

	endSec := int64(startScore) + windowSec
	tSec := latest.Unix()
	if tSec <= endSec {
		return nil
	}

	locked, err := w.redisClient.SetNX(ctx, w.lockKey(), "1", 2*time.Minute).Result()
	if err != nil || !locked {
		return nil
	}
	defer w.redisClient.Del(ctx, w.lockKey())

	for tSec > endSec {
		windowStart := endSec - windowSec
		w.triggerWorker(ctx, time.Unix(windowStart, 0).UTC(), time.Unix(endSec, 0).UTC())
		endSec += slideSec
	}

	return w.setWindowStart(ctx, w.query.Name, endSec-windowSec)
}

// handleSession groups events for the query's configured SessionID into windows separated by
// gaps of at least sessionGapSeconds, closing (and triggering) a session once it's been quiet
// for that long.
func (w *Windower) handleSession(ctx context.Context, latest time.Time) error {
	id := w.query.SessionID
	if id == "" {
		return fmt.Errorf("session window %q missing session_id in config", w.query.Name)
	}

	timesKey := fmt.Sprintf("%s:%s", sessionKeyPrefix, id)
	progressMember := w.query.Name + ":" + id

	scores, err := w.redisClient.ZRangeWithScores(ctx, timesKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("redis zrange failed: %w", err)
	}
	if len(scores) == 0 {
		w.redisClient.ZRem(ctx, windowNextKey, progressMember)
		return nil
	}

	locked, err := w.redisClient.SetNX(ctx, w.lockKey(), "1", 2*time.Minute).Result()
	if err != nil || !locked {
		return nil
	}
	defer w.redisClient.Del(ctx, w.lockKey())

	windowStart := int64(scores[0].Score)
	windowEnd := windowStart
	for i := 1; i < len(scores); i++ {
		next := int64(scores[i].Score)
		if next-windowEnd <= sessionGapSeconds {
			windowEnd = next
			continue
		}
		w.triggerWorker(ctx, time.Unix(windowStart, 0).UTC(), time.Unix(windowEnd, 0).UTC())
		windowStart = next
		windowEnd = next
	}

	if latest.Unix()-windowEnd > sessionGapSeconds {
		w.triggerWorker(ctx, time.Unix(windowStart, 0).UTC(), time.Unix(windowEnd, 0).UTC())
		w.redisClient.ZRemRangeByScore(ctx, timesKey, "-inf", strconv.FormatInt(windowEnd, 10))
		w.redisClient.ZRem(ctx, windowNextKey, progressMember)
		return nil
	}

	w.redisClient.ZRemRangeByScore(ctx, timesKey, "-inf", "("+strconv.FormatInt(windowStart, 10))
	return w.setWindowStart(ctx, progressMember, windowStart)
}

// triggerWorker spawns a worker call for the given window, skipping windows with no ingested data.
func (w *Windower) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)

	count, err := w.redisClient.ZCount(ctx, redisStreamKey, minScore, maxScore).Result()
	if err != nil {
		log.Printf("[Windower:%s] ERROR checking window data: %v\n", w.query.Name, err)
		return
	}
	if count == 0 {
		log.Printf("[Windower:%s] No data in window %s - %s, skipping worker trigger\n", w.query.Name, windowStart.Format("15:04:05"), windowEnd.Format("15:04:05"))
		return
	}

	log.Printf("[Windower:%s] Triggering worker for window (scores): %s(%s) - %s(%s)\n", w.query.Name, minScore, windowStart.Format("15:04:05"), maxScore, windowEnd.Format("15:04:05"))
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
