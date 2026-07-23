package windower

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
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/mardentub/windower/config"
	"github.com/redis/go-redis/v9"
)

const (
	windowNextKey     = "window:next"
	dataKey           = "data"
	lockKey           = "lock"
	sessionKey        = "session"
	activeKey         = "active"
	lateBufferSeconds = 3
)

var coord *Coordinator

type Coordinator struct {
	rdb    *redis.Client
	config *config.Config
}

func NewCoordinator(rdb *redis.Client, cfg *config.Config) *Coordinator {
	return &Coordinator{
		rdb:    rdb,
		config: cfg,
	}
}

var recordSetWindowStart = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

var cleanupBelowMin = redis.NewScript(`
local lo = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if lo[2] == nil then
    return {0, 0, 0, 0}
end
local minStr = lo[2]
local first = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', '(' .. minStr, 'WITHSCORES', 'LIMIT', 0, 1)
local last = redis.call('ZREVRANGEBYSCORE', KEYS[2], '(' .. minStr, '-inf', 'WITHSCORES', 'LIMIT', 0, 1)
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. minStr)
local minRemovedScore = 0
local maxRemovedScore = 0
if #first > 0 then
    minRemovedScore = tonumber(first[2])
    maxRemovedScore = tonumber(last[2])
end
return {tonumber(minStr), removed, minRemovedScore, maxRemovedScore}
`)

func init() {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatalln("REDIS_URL is empty")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		PoolSize: 50,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("Redis not reachable at %s: %v", redisURL, err)
	}

	cfg := config.LoadConfig()

	coord = NewCoordinator(rdb, &cfg)

	functions.HTTP("ProcessWindows", processWindows)
}

func processWindows(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Add(-lateBufferSeconds * time.Second)
	ctx := context.Background()

	var wg sync.WaitGroup
	errCh := make(chan error, len(coord.config.Queries))

	for _, q := range coord.config.Queries {

		_, exists := coord.config.Sources[q.DataSource]
		if q.DataSource != "generic" && !exists {
			log.Printf("[Windower] Skipping query %q: source %q is not active/defined in 'sources'", q.Name, q.DataSource)
			continue
		}

		wg.Add(1)
		go func(q config.Query) {
			defer wg.Done()
			var err error
			switch q.WindowType {
			case "tumbling":
				err = coord.handleTumbling(ctx, now, q)
			case "sliding":
				err = coord.handleSliding(ctx, now, q)
			/*case "session":
			err = coord.handleSession(ctx, now, q)*/
			default:
				err = fmt.Errorf("unsupported window type %q (%s)", q.WindowType, q.Name)
			}
			if err != nil {
				errCh <- err
			}
		}(q)
	}
	wg.Wait()
	close(errCh)
	for sourceName := range coord.config.Sources {
		coord.recordCleanup(ctx, sourceName)
	}

	hasError := false
	for err := range errCh {
		if err != nil {
			log.Printf("[Windower] query failed: %v", err)
			hasError = true
		}
	}
	if hasError {
		http.Error(w, "one or more queries failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Coordinator) triggerWorker(windowStart, windowEnd time.Time, q config.Query, id string) error {

	source, _ := c.config.Sources[q.DataSource]

	data := map[string]interface{}{
		"window_start":     windowStart.Unix(),
		"window_end":       windowEnd.Unix(),
		"query":            q.Query,
		"query_name":       q.Name,
		"return_type":      q.ReturnType,
		"id":               id,
		"is_alert":         q.IsAlert,
		"alert_format":     q.AlertFormat,
		"data_source":      q.DataSource,
		"columns":          source.Columns,
		"reference_tables": source.ReferenceTables,
	}
	payload, _ := json.Marshal(data)
	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Post(os.Getenv("WORKER_URL"), "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("trigger worker %s failed: %w", q.Name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("trigger worker %s: unexpected status %d", q.Name, resp.StatusCode)
	}
	return nil
}

func (c *Coordinator) recordCleanup(ctx context.Context, prefix string) {
	specificDataKey := dataKey + ":" + prefix
	specificWindowNextKey := windowNextKey + ":" + prefix
	res, err := cleanupBelowMin.Run(ctx, c.rdb,
		[]string{specificWindowNextKey, specificDataKey},
	).Result()
	if err != nil {
		log.Printf("[Cleanup] failed: %v", err)
		return
	}
	vals, ok := res.([]interface{})
	if !ok || len(vals) != 4 {
		return
	}
	minScore, removed, minRemoved, maxRemoved := vals[0].(int64), vals[1].(int64), vals[2].(int64), vals[3].(int64)
	if removed > 0 {
		log.Printf("[Cleanup] source=%s pruned %d event(s) below min_window_start=%d, removed_score_range=[%d,%d]",
			prefix, removed, minScore, minRemoved, maxRemoved)
	}
}

// tumbling: slideSecs == windowSec
func (c *Coordinator) createWindows(ctx context.Context, t time.Time, q config.Query, slideSecs int64) error {
	prefix := q.DataSource
	lockKeyWindow := lockKey + ":" + prefix + ":" + q.Name

	ok, _ := c.rdb.SetNX(ctx, lockKeyWindow, "locked", 2*time.Minute).Result()
	if !ok {
		return nil
	}

	var updateErr error
	func() {
		defer c.rdb.Del(ctx, lockKeyWindow)

		specificDataKey := dataKey + ":" + prefix
		specificWindowNextKey := windowNextKey + ":" + prefix
		startScore, err := c.rdb.ZScore(ctx, specificWindowNextKey, q.Name).Result()
		windowSec := int64(q.WindowSize)

		if errors.Is(err, redis.Nil) {
			startSec := t.Unix()
			if err := recordSetWindowStart.Run(ctx, c.rdb,
				[]string{specificWindowNextKey},
				strconv.FormatInt(startSec, 10),
				q.Name,
			).Err(); err != nil {
				updateErr = fmt.Errorf("redis init window failed: %w", err)
			}
			return
		}

		if err != nil {
			updateErr = fmt.Errorf("redis zscore failed: %w", err)
			return
		}

		startSec := int64(startScore)
		endSec := startSec + windowSec
		tSec := t.Unix()

		if tSec <= endSec {
			return
		}

		var workerWg sync.WaitGroup

		for tSec > endSec {
			winStart := endSec - windowSec
			count, _ := c.rdb.ZCount(ctx, specificDataKey,
				strconv.FormatInt(winStart, 10),
				"("+strconv.FormatInt(endSec, 10)).Result()
			if count > 0 {
				workerWg.Add(1)
				go func(start, end int64) {
					defer workerWg.Done()
					if err := c.triggerWorker(time.Unix(start, 0).UTC(), time.Unix(end, 0).UTC(), q, ""); err != nil {
						log.Printf("[Coordinator] trigger worker %s failed: %v", q.Name, err)
					}
				}(winStart, endSec)
			} else {
				log.Printf("[Coordinator] EMPTY window query=%s source=%s range=[%d,%d) at check_time=%d",
					q.Name, prefix, winStart, endSec, tSec)
			}
			// tumbling: slideSecs == windowSec
			endSec += slideSecs
		}

		workerWg.Wait()

		startSec = endSec - windowSec

		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{specificWindowNextKey},
			strconv.FormatInt(startSec, 10),
			q.Name,
		).Err(); err != nil {
			updateErr = fmt.Errorf("redis set window failed: %w", err)
		}
	}()
	return updateErr
}

func (c *Coordinator) handleTumbling(ctx context.Context, t time.Time, q config.Query) error {
	return c.createWindows(ctx, t, q, int64(q.WindowSize))
}

func (c *Coordinator) handleSliding(ctx context.Context, t time.Time, q config.Query) error {
	const slideSeconds = 60
	slideSecs := int64(slideSeconds) // int64(q.SlideInSeconds)
	return c.createWindows(ctx, t, q, slideSecs)
}

func (c *Coordinator) handleSession(ctx context.Context, t time.Time, q config.Query) error {
	prefix := q.DataSource
	specificWindowNextKey := windowNextKey + ":" + prefix
	specificSessionKey := sessionKey + ":" + prefix
	specificActiveKey := activeKey + ":" + prefix
	const sessionGapSeconds = 300
	gapSec := int64(sessionGapSeconds)
	nowSec := t.Unix()

	ids, err := c.rdb.SMembers(ctx, specificActiveKey).Result()
	if err != nil {
		return fmt.Errorf("redis smembers failed: %w", err)
	}

	for _, id := range ids {

		timesKey := specificSessionKey + ":" + id
		lockKeySession := lockKey + ":" + prefix + ":" + q.Name + ":" + id
		memberStart := q.Name + ":" + id + ":start"

		ok, _ := c.rdb.SetNX(ctx, lockKeySession, "locked", 2*time.Minute).Result()
		if !ok {
			continue
		}

		func(currentID, currentTimesKey, currentLockKey, currentMemberStart string) {
			defer c.rdb.Del(ctx, currentLockKey)

			scores, err := c.rdb.ZRangeWithScores(ctx, currentTimesKey, 0, -1).Result()
			if err != nil {
				log.Printf("[Session] failed to get scores for id %s: %v", currentID, err)
				return
			}

			if len(scores) == 0 {
				c.rdb.ZRem(ctx, specificWindowNextKey, currentMemberStart)
				c.rdb.SRem(ctx, specificActiveKey, currentID)
				return
			}

			winStart := int64(scores[0].Score)
			winEnd := int64(scores[0].Score)

			var workerWg sync.WaitGroup

			for i := 1; i < len(scores); i++ {
				nextEvent := int64(scores[i].Score)
				diff := nextEvent - winEnd
				if diff <= gapSec {
					winEnd = nextEvent
				} else {
					workerWg.Add(1)
					go func(start, end int64, query config.Query, id string) {
						defer workerWg.Done()
						if err := c.triggerWorker(time.Unix(start, 0).UTC(), time.Unix(end, 0).UTC(), query, id); err != nil {
							log.Printf("[Session] failed to trigger worker for id %s: %v", id, err)
						}
					}(winStart, winEnd, q, currentID)
					winStart = int64(scores[i].Score)
					winEnd = int64(scores[i].Score)
				}
			}

			workerWg.Wait()

			diff := nowSec - winEnd
			if diff > gapSec {
				if err := c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(winEnd, 0).UTC(), q, currentID); err != nil {
					log.Printf("[Session] failed to trigger window for id %s: %v", currentID, err)
				}

				c.rdb.ZRemRangeByScore(ctx, currentTimesKey, "-inf", strconv.FormatInt(winEnd, 10))
				c.rdb.ZRem(ctx, specificWindowNextKey, currentMemberStart)
				c.rdb.SRem(ctx, specificActiveKey, currentID)
				return
			}

			if err := recordSetWindowStart.Run(ctx, c.rdb,
				[]string{specificWindowNextKey},
				strconv.FormatInt(winStart, 10),
				currentMemberStart,
			).Err(); err != nil {
				log.Printf("[Session] redis set window start failed for %s: %v", currentID, err)
			}

			c.rdb.ZRemRangeByScore(ctx, currentTimesKey, "-inf", "("+strconv.FormatInt(winStart, 10))
		}(id, timesKey, lockKeySession, memberStart)
	}

	return nil
}
