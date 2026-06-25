package main

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
	"github.com/mardentub/coordinator/config"
	"github.com/redis/go-redis/v9"
)

const (
	windowNextKey     = "window:next"
	dataKey           = "data"
	lockKey           = "lock"
	sessionKey        = "sessionKey"
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
    return {0, 0}
end
local minStr = lo[2]
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. minStr)
return {tonumber(minStr), removed}
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
	ctx := r.Context()

	var wg sync.WaitGroup
	errCh := make(chan error, len(coord.config.Queries))

	for _, q := range coord.config.Queries {

		_, exists := coord.config.Sources[q.DataSource]
		if !exists {
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

	for err := range errCh {
		if err != nil {
			log.Printf("[Windower] tick failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Coordinator) triggerWorker(windowStart, windowEnd time.Time, q config.Query, id string) {

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
	dataBytes, _ := json.Marshal(data)
	go func(payload []byte, name string) {
		client := &http.Client{Timeout: 30 * time.Second}

		resp, err := client.Post(os.Getenv("WORKER_URL"), "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("[Coordinator] Trigger failed for query %s: %v", name, err)
			return
		}
		defer resp.Body.Close()
		log.Printf("[Coordinator:%s] Worker spawned", name)

	}(dataBytes, q.Name)
}

func (c *Coordinator) recordCleanup(ctx context.Context, prefix string) {
	specificDataKey := dataKey + ":" + prefix
	specificWindowNextKey := windowNextKey + ":" + prefix
	_, err := cleanupBelowMin.Run(ctx, c.rdb,
		[]string{specificWindowNextKey, specificDataKey},
	).Result()
	if err != nil {
		log.Printf("[Cleanup] failed: %v", err)
	}
}

func (c *Coordinator) handleTumbling(ctx context.Context, t time.Time, q config.Query) error {
	prefix := q.DataSource
	lockKeyTumbling := lockKey + ":" + prefix + ":" + q.Name
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
			return fmt.Errorf("redis init window failed: %w", err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("redis zscore failed: %w", err)
	}

	startSec := int64(startScore)
	endSec := startSec + windowSec
	tSec := t.Unix()

	if tSec <= endSec {
		return nil
	}
	ok, _ := c.rdb.SetNX(ctx, lockKeyTumbling, "locked", 2*time.Minute).Result()
	if !ok {
		return nil
	}

	var updateErr error
	func() {
		defer c.rdb.Del(ctx, lockKeyTumbling)

		for tSec > endSec {
			winStart := endSec - windowSec
			count, _ := c.rdb.ZCount(ctx, specificDataKey,
				strconv.FormatInt(winStart, 10),
				strconv.FormatInt(endSec, 10)).Result()
			if count > 0 {
				c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(endSec, 0).UTC(), q, "")
			}

			endSec += windowSec
		}

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

func (c *Coordinator) handleSliding(ctx context.Context, t time.Time, q config.Query) error {
	prefix := q.DataSource
	lockKeySliding := lockKey + ":" + prefix + ":" + q.Name
	specificDataKey := dataKey + ":" + prefix
	specificWindowNextKey := windowNextKey + ":" + prefix
	startScore, err := c.rdb.ZScore(ctx, specificWindowNextKey, q.Name).Result()
	windowSec := int64(q.WindowSize)
	const slideSeconds = 60
	slideSecs := int64(slideSeconds) // int64(q.SlideInSeconds)

	if errors.Is(err, redis.Nil) {
		startSec := t.Unix()
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{specificWindowNextKey},
			strconv.FormatInt(startSec, 10),
			q.Name,
		).Err(); err != nil {
			return fmt.Errorf("redis init window failed: %w", err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("redis zscore failed: %w", err)
	}

	startSec := int64(startScore)
	endSec := startSec + windowSec
	tSec := t.Unix()

	if tSec <= endSec {
		return nil
	}
	ok, _ := c.rdb.SetNX(ctx, lockKeySliding, "locked", 2*time.Minute).Result()
	if !ok {
		return nil
	}

	var updateErr error
	func() {
		defer c.rdb.Del(ctx, lockKeySliding)

		for tSec > endSec {
			winStart := endSec - windowSec
			count, _ := c.rdb.ZCount(ctx, specificDataKey,
				strconv.FormatInt(winStart, 10),
				strconv.FormatInt(endSec, 10)).Result()
			if count > 0 {
				c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(endSec, 0).UTC(), q, "")
			}

			endSec += slideSecs
		}

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

func (c *Coordinator) handleSession(ctx context.Context, t time.Time, q config.Query) error {
	prefix := q.DataSource
	specificWindowNextKey := windowNextKey + ":" + prefix
	specificSessionKey := sessionKey + ":" + prefix
	const sessionGapSeconds = 300
	gapSec := int64(sessionGapSeconds)
	nowSec := t.Unix()

	keys, err := c.rdb.Keys(ctx, specificSessionKey+":*").Result()
	if err != nil {
		return fmt.Errorf("redis keys failed: %w", err)
	}

	for _, timesKey := range keys {

		id := timesKey[len(specificSessionKey)+1:]

		lockKeySession := lockKey + ":" + prefix + ":" + q.Name + ":" + id
		memberStart := q.Name + ":" + id + ":start"

		scores, err := c.rdb.ZRangeWithScores(ctx, timesKey, 0, -1).Result()
		if err != nil {
			log.Printf("[Session] failed to get scores for id %s: %v", id, err)
			continue
		}

		if len(scores) == 0 {
			c.rdb.ZRem(ctx, specificWindowNextKey, memberStart)
			continue
		}

		ok, _ := c.rdb.SetNX(ctx, lockKeySession, "locked", 2*time.Minute).Result()
		if !ok {
			continue
		}

		func(scores []redis.Z, currentID, currentTimesKey, currentLockKey, currentMemberStart string) {
			defer c.rdb.Del(ctx, currentLockKey)

			winStart := int64(scores[0].Score)
			winEnd := int64(scores[0].Score)

			for i := 1; i < len(scores); i++ {
				nextEvent := int64(scores[i].Score)
				diff := nextEvent - winEnd
				if diff <= gapSec {
					winEnd = nextEvent
				} else {
					c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(winEnd, 0).UTC(), q, currentID)
					winStart = int64(scores[i].Score)
					winEnd = int64(scores[i].Score)
				}
			}

			diff := nowSec - winEnd
			if diff > gapSec {
				c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(winEnd, 0).UTC(), q, currentID)
				c.rdb.ZRemRangeByScore(ctx, currentTimesKey, "-inf", strconv.FormatInt(winEnd, 10))
				c.rdb.ZRem(ctx, specificWindowNextKey, currentMemberStart)
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
		}(scores, id, timesKey, lockKeySession, memberStart)
	}

	return nil
}
