package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
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
	config *Config
}

func NewCoordinator(rdb *redis.Client, cfg *Config) *Coordinator {
	return &Coordinator{
		rdb:    rdb,
		config: cfg,
	}
}

type Query struct {
	Name       string `yaml:"name"`
	WindowType string `yaml:"window_type"`
	WindowSize int    `yaml:"window_size"`
	Query      string `yaml:"query"`
	ReturnType string `yaml:"return_type"`
	Id         string `yaml:"id"`
}

type Config struct {
	Queries []Query `yaml:"queries"`
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

func readGCSFile(ctx context.Context, bucket, object string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("GCS read %s/%s: %w", bucket, object, err)
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

func loadConfig(ctx context.Context) (*Config, error) {
	bucket := os.Getenv("CONFIG_BUCKET")
	object := os.Getenv("CONFIG_OBJECT")
	if bucket == "" || object == "" {
		return nil, fmt.Errorf("CONFIG_BUCKET or CONFIG_OBJECT is empty")
	}
	data, err := readGCSFile(ctx, bucket, object)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("yaml parse: %w", err)
	}
	if len(cfg.Queries) == 0 {
		return nil, errors.New("no queries defined in config")
	}
	return &cfg, nil
}

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

	cfg, err := loadConfig(ctx)
	if err != nil {
		log.Fatalf("Could not load config in init-block: %v", err)
	}

	coord = NewCoordinator(rdb, cfg)

	functions.HTTP("ProcessWindows", processWindows)
}

func processWindows(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Add(-lateBufferSeconds * time.Second)
	ctx := r.Context()

	var wg sync.WaitGroup
	errCh := make(chan error, len(coord.config.Queries))

	for _, q := range coord.config.Queries {
		wg.Add(1)
		go func(q Query) {
			defer wg.Done()
			var err error
			switch q.WindowType {
			case "tumbling":
				err = coord.handleTumbling(ctx, now, q)
			case "sliding":
				err = coord.handleSliding(ctx, now, q)
			case "session":
				err = coord.handleSession(ctx, now, q)
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
	coord.recordCleanup(ctx)

	for err := range errCh {
		if err != nil {
			log.Printf("[Windower] tick failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Coordinator) triggerWorker(windowStart, windowEnd time.Time, q Query, id string) {
	data := map[string]interface{}{
		"window_start": windowStart.Unix(),
		"window_end":   windowEnd.Unix(),
		"query":        q.Query,
		"query_name":   q.Name,
		"return_type":  q.ReturnType,
		"id":           id,
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

	}(dataBytes, q.Name)
}

func (c *Coordinator) recordCleanup(ctx context.Context) {
	_, err := cleanupBelowMin.Run(ctx, c.rdb,
		[]string{windowNextKey, dataKey},
	).Result()
	if err != nil {
		log.Printf("[Cleanup] failed: %v", err)
	}
}

func (c *Coordinator) handleTumbling(ctx context.Context, t time.Time, q Query) error {
	lockKeyTumbling := lockKey + q.Name
	startScore, err := c.rdb.ZScore(ctx, windowNextKey, q.Name).Result()
	windowSec := int64(q.WindowSize)

	if errors.Is(err, redis.Nil) {
		startSec := t.Unix()
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
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
			count, _ := c.rdb.ZCount(ctx, dataKey,
				strconv.FormatInt(winStart, 10),
				strconv.FormatInt(endSec, 10)).Result()
			if count > 0 {
				c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(endSec, 0).UTC(), q, "")
			}

			endSec += windowSec
		}

		startSec = endSec - windowSec

		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(startSec, 10),
			q.Name,
		).Err(); err != nil {
			updateErr = fmt.Errorf("redis set window failed: %w", err)
		}
	}()
	return updateErr
}

func (c *Coordinator) handleSliding(ctx context.Context, t time.Time, q Query) error {
	lockKeySliding := lockKey + q.Name
	startScore, err := c.rdb.ZScore(ctx, windowNextKey, q.Name).Result()
	windowSec := int64(q.WindowSize)
	const slideSeconds = 60
	slideSecs := int64(slideSeconds) // int64(q.SlideInSeconds)

	if errors.Is(err, redis.Nil) {
		startSec := t.Unix()
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
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
			count, _ := c.rdb.ZCount(ctx, dataKey,
				strconv.FormatInt(winStart, 10),
				strconv.FormatInt(endSec, 10)).Result()
			if count > 0 {
				c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(endSec, 0).UTC(), q, "")
			}

			endSec += slideSecs
		}

		startSec = endSec - windowSec

		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(startSec, 10),
			q.Name,
		).Err(); err != nil {
			updateErr = fmt.Errorf("redis set window failed: %w", err)
		}
	}()
	return updateErr

}

func (c *Coordinator) handleSession(ctx context.Context, t time.Time, q Query) error {
	id := q.Id
	const sessionGapSeconds = 300
	gapSec := int64(sessionGapSeconds)

	lockKeySession := lockKey + q.Name + id
	memberStart := q.Name + ":" + id + ":start"
	timesKey := sessionKey + ":" + id
	nowSec := t.Unix()

	scores, err := c.rdb.ZRangeWithScores(ctx, timesKey, 0, -1).Result()

	if err != nil {
		return fmt.Errorf("redis zrangebyscore failed: %w", err)
	}

	if len(scores) == 0 {
		c.rdb.ZRem(ctx, windowNextKey, memberStart)
		return nil
	}

	ok, _ := c.rdb.SetNX(ctx, lockKeySession, "locked", 2*time.Minute).Result()
	if !ok {
		return nil
	}

	var updateErr error
	func(scores []redis.Z) {
		defer c.rdb.Del(ctx, lockKeySession)

		winStart := int64(scores[0].Score)
		winEnd := int64(scores[0].Score)

		for i := 1; i < len(scores); i++ {
			nextEvent := int64(scores[i].Score)
			diff := nextEvent - winEnd
			if diff <= gapSec {
				winEnd = nextEvent
			} else {
				c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(winEnd, 0).UTC(), q, id)
				winStart = int64(scores[i].Score)
				winEnd = int64(scores[i].Score)
			}
		}

		diff := nowSec - winEnd
		if diff > gapSec {
			c.triggerWorker(time.Unix(winStart, 0).UTC(), time.Unix(winEnd, 0).UTC(), q, id)
			c.rdb.ZRemRangeByScore(ctx, timesKey, "-inf", strconv.FormatInt(winEnd, 10))
			c.rdb.ZRem(ctx, windowNextKey, memberStart)
			return
		}
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(winStart, 10),
			memberStart,
		).Err(); err != nil {
			updateErr = fmt.Errorf("redis set window start failed: %w", err)
		}
		c.rdb.ZRemRangeByScore(ctx, timesKey, "-inf", "("+strconv.FormatInt(winStart, 10))
	}(scores)
	return updateErr
}
