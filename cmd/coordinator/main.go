package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

const redisStreamKey = "ais-stream"

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

type QueryConfig struct {
	Name                string `yaml:"name"`
	WindowType          string `yaml:"window_type"`
	WindowSizeInSeconds int    `yaml:"window_size"`
	SQLQueries          []struct {
		Name  string `yaml:"name"`
		Query string `yaml:"query"`
	} `yaml:"sql"`
}

type Config struct {
	Queries []QueryConfig `yaml:"queries"`
}

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
	data, err := readGCSFile(ctx, "faastreams-config", "ais-stream-config.template.yaml")
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("yaml parse: %w", err)
	}
	return &cfg, nil
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
	appConfig  *Config
	configOnce sync.Once
	rdb        *redis.Client
)

func init() {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	rdb = redis.NewClient(&redis.Options{
		Addr:     redisURL,
		PoolSize: 20,
	})
	var err error
	appConfig, err = loadConfig(ctx)
	if err != nil {
		log.Fatalf("Could not load config in init-block: %v", err)
	}
	functions.CloudEvent("ProcessAisData", processAisData)
}

func processAisData(ctx context.Context, e event.Event) error {
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

	t, err := time.Parse(time.RFC3339, ais.Timestamp)
	if err != nil {
		return fmt.Errorf("timestamp format error: %w", err)
	}

	c.handleEvent(ctx, t, pubSubMessage.Message.Data)

	return nil
}

func (c *Coordinator) handleEvent(ctx context.Context, t time.Time, rawData []byte) error {
	for _, q := range c.config.Queries {
		windowKey := fmt.Sprintf("ais:window:%s:end", q.Name)

		err := c.rdb.ZAdd(ctx, fmt.Sprintf("ais:data:%s", q.Name), redis.Z{
			Score:  float64(t.Unix()),
			Member: rawData,
		}).Err()
		if err != nil {
			return fmt.Errorf("redis zadd failed: %w", err)
		}

		endTimeStr, _ := c.rdb.Get(ctx, windowKey).Result()
		if endTimeStr == "" {
			lockKey := fmt.Sprintf("ais:lock:init:%s", q.Name)
			ok, _ := c.rdb.SetNX(ctx, lockKey, "locked", 2*time.Second).Result()
			if ok {
				var innerErr error
				func() {
					defer c.rdb.Del(ctx, lockKey)

					val, e := c.rdb.Get(ctx, windowKey).Result()
					if e != nil && e != redis.Nil {
						innerErr = fmt.Errorf("redis double-check failed: %w", e)
						return
					}

					if val == "" {
						newEnd := t.Add(time.Duration(q.WindowSizeInSeconds) * time.Second)
						setErr := c.rdb.Set(ctx, windowKey, newEnd.Format(time.RFC3339), 0).Err()
						if setErr != nil {
							innerErr = fmt.Errorf("redis set newEnd failed: %w", setErr)
						}
					}
				}()
				if innerErr != nil {
					return innerErr
				}
			}
			continue
		}

		endTime, _ := time.Parse(time.RFC3339, endTimeStr)
		if t.After(endTime) {
			lockKey := fmt.Sprintf("ais:lock:update:%s", q.Name)
			ok, _ := c.rdb.SetNX(ctx, lockKey, "locked", 2*time.Second).Result()
			if ok {
				var updateErr error
				func() {
					defer c.rdb.Del(ctx, lockKey)

					startTime := endTime.Add(-time.Duration(q.WindowSizeInSeconds) * time.Second)
					c.triggerWorker(startTime, endTime, q)

					buffer := time.Duration(q.WindowSizeInSeconds) * time.Second
					cleanupLimit := endTime.Add(-2 * buffer)
					limit := float64(cleanupLimit.Unix())
					e := c.rdb.ZRemRangeByScore(ctx, fmt.Sprintf("ais:data:%s", q.Name), "-inf", fmt.Sprintf("%f", limit)).Err()
					if e != nil {
						updateErr = fmt.Errorf("redis ZRemRangeByScore failed: %w", e)
					}

					newEnd := t.Add(time.Duration(q.WindowSizeInSeconds) * time.Second)
					e = c.rdb.Set(ctx, windowKey, newEnd.Format(time.RFC3339), 0).Err()
					if e != nil {
						updateErr = fmt.Errorf("redis window update failed: %w", e)
					}
				}()
				if updateErr != nil {
					return updateErr
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) triggerWorker(windowStart, windowEnd time.Time, q QueryConfig) {
	for _, query := range q.SQLQueries {
		data := map[string]interface{}{
			"window_start": windowStart.Unix(),
			"window_end":   windowEnd.Unix(),
			"query":        query.Query,
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

			log.Printf("[Coordinator] Worker triggered successfully for query: %s", name)
		}(dataBytes, query.Name)
	}
}
