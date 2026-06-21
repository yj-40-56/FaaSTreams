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
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

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
}

type Config struct {
	Queries []Query `yaml:"queries"`
}

type AISData struct {
	Timestamp          string  `json:"timestamp"`
	MMSI               string  `json:"mmsi"`
	Name               string  `json:"name"`
	IMO                string  `json:"imo"`
	Callsign           string  `json:"callsign"`
	Latitude           float64 `json:"latitude"`
	Longitude          float64 `json:"longitude"`
	SOG                float64 `json:"sog"`
	COG                float64 `json:"cog"`
	Heading            float64 `json:"heading"`
	ROT                float64 `json:"rot"`
	ShipType           string  `json:"shipType"`
	NavigationalStatus string  `json:"navigationalStatus"`
	Destination        string  `json:"destination"`
	ETA                string  `json:"eta"`
	Draught            float64 `json:"draught"`
	Length             float64 `json:"length"`
	Width              float64 `json:"width"`
	CargoType          string  `json:"cargoType"`
	TypeOfMobile       string  `json:"typeOfMobile"`
	PositionFixDevice  string  `json:"positionFixingDevice"`
	DataSourceType     string  `json:"dataSourceType"`
	A                  float64 `json:"a"`
	B                  float64 `json:"b"`
	C                  float64 `json:"c"`
	D                  float64 `json:"d"`
}

var (
	appConfig *Config
	rdb       *redis.Client
)

const (
	windowNextKey = "window:next"
	dataKey       = "data"
	lockKey       = "lock"
)

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
	rdb = redis.NewClient(&redis.Options{
		Addr:     redisURL,
		PoolSize: 50,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("Redis not reachable at %s: %v", redisURL, err)
	}
	var err error
	appConfig, err = loadConfig(ctx)
	if err != nil {
		log.Fatalf("Could not load config in init-block: %v", err)
	}

	functions.CloudEvent("ProcessData", processData)
}

func processData(ctx context.Context, e event.Event) error {
	c := NewCoordinator(rdb, appConfig)

	var pubSubMessage struct {
		Message struct {
			Data []byte `json:"data"`
		} `json:"message"`
	}

	if err := json.Unmarshal(e.Data(), &pubSubMessage); err != nil {
		return fmt.Errorf("pubsub envelope parse error: %w", err)
	}

	var ais AISData
	if err := json.Unmarshal(pubSubMessage.Message.Data, &ais); err != nil {
		return fmt.Errorf("ais schema error: %w", err)
	}

	t, err := time.Parse("02/01/2006 15:04:05", ais.Timestamp)
	if err != nil {
		return fmt.Errorf("timestamp format error: %w", err)
	}

	c.handleEvent(ctx, t, pubSubMessage.Message.Data, ais.MMSI)

	return nil
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

func (c *Coordinator) handleEvent(ctx context.Context, t time.Time, rawData []byte, id string) error {
	if err := c.rdb.ZAdd(ctx, dataKey, redis.Z{
		Score:  float64(t.Unix()),
		Member: string(rawData),
	}).Err(); err != nil {
		return fmt.Errorf("redis zadd failed: %w", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(c.config.Queries))

	for _, q := range c.config.Queries {
		wg.Add(1)
		go func(q Query) {
			defer wg.Done()

			var err error
			switch q.WindowType {
			case "tumbling":
				err = c.handleTumbling(ctx, t, q)
			case "sliding":
				err = c.handleSliding(ctx, t, q)
			case "session":
				err = c.handleSession(ctx, t, q, id)
			default:
				err = fmt.Errorf("unknown window type %q (%s)", q.WindowType, q.Name)
			}
			if err != nil {
				errCh <- err
			}
		}(q)
	}

	wg.Wait()
	close(errCh)

	c.recordCleanup(ctx)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
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

func (c *Coordinator) handleSession(ctx context.Context, t time.Time, q Query, id string) error {
	lockKeySession := lockKey + q.Name + id
	memberStart := q.Name + ":" + id + ":start"
	memberLast := q.Name + ":" + id + ":last"
	startScore, errStart := c.rdb.ZScore(ctx, windowNextKey, memberStart).Result()
	lastScore, _ := c.rdb.ZScore(ctx, windowNextKey, memberLast).Result()
	startSec := int64(startScore)
	lastSec := int64(lastScore)
	const sessionGapSeconds = 300
	gapSec := int64(sessionGapSeconds)

	if errors.Is(errStart, redis.Nil) {
		startSec := t.Unix()
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(startSec, 10),
			memberStart,
		).Err(); err != nil {
			return fmt.Errorf("redis init window start failed: %w", err)
		}
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(startSec, 10),
			memberLast,
		).Err(); err != nil {
			return fmt.Errorf("redis init window last failed: %w", err)
		}
		return nil
	}

	tSec := t.Unix()

	if tSec-lastSec <= gapSec {
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(tSec, 10),
			memberLast,
		).Err(); err != nil {
			return fmt.Errorf("redis update window last failed: %w", err)
		}
		return nil
	}

	ok, _ := c.rdb.SetNX(ctx, lockKeySession, "locked", 2*time.Minute).Result()
	if !ok {
		return nil
	}

	var updateErr error
	func() {
		defer c.rdb.Del(ctx, lockKeySession)

		count, _ := c.rdb.ZCount(ctx, dataKey,
			strconv.FormatInt(startSec, 10),
			strconv.FormatInt(lastSec, 10)).Result()
		if count > 0 {
			c.triggerWorker(time.Unix(startSec, 0).UTC(), time.Unix(lastSec, 0).UTC(), q, id)
		}
		newStartSec := t.Unix()
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(newStartSec, 10),
			memberStart,
		).Err(); err != nil {
			updateErr = fmt.Errorf("redis set window start failed: %w", err)
		}
		if err := recordSetWindowStart.Run(ctx, c.rdb,
			[]string{windowNextKey},
			strconv.FormatInt(newStartSec, 10),
			memberLast,
		).Err(); err != nil {
			updateErr = fmt.Errorf("redis set window last failed: %w", err)
		}
	}()
	return updateErr
}
