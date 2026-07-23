package ingestor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/mardentub/ingestor/config"
	"github.com/redis/go-redis/v9"
)

const (
	dataKey    = "data"
	sessionKey = "session"
	activeKey  = "active"
)

var (
	rdb       *redis.Client
	appConfig config.Config
)

func init() {

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatalln("REDIS_URL is empty")
	}

	appConfig = config.LoadConfig()

	rdb = redis.NewClient(&redis.Options{
		Addr:     redisURL,
		PoolSize: 50,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis not reachable at %s: %v", redisURL, err)
	}

	functions.CloudEvent("IngestEvent", ingestEvent)
}

func ingestEvent(ctx context.Context, e event.Event) error {
	var pubSubMessage struct {
		Message struct {
			Data []byte `json:"data"`
		} `json:"message"`
	}
	if err := json.Unmarshal(e.Data(), &pubSubMessage); err != nil {
		return fmt.Errorf("pubsub envelope parse error: %w", err)
	}

	return processMessage(ctx, pubSubMessage.Message.Data)
}

// eventRecord is the Redis write derived from a single parsed event: which
// sorted set it belongs to, and the ZADD member/score to write into it.
type eventRecord struct {
	key string
	z   redis.Z
}

// parseEvent validates a single event's raw published bytes and turns it into
// the Redis write it should produce. ok=false (with a nil error) means the
// message was malformed/unroutable and should be dropped (acked), not
// retried; a non-nil error means a transient/schema failure worth retrying.
func parseEvent(data []byte) (rec eventRecord, ok bool, err error) {
	var fields map[string]interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return eventRecord{}, false, fmt.Errorf("event schema error: %w", err)
	}

	sourceName, ok := fields["_source"].(string)
	if !ok || sourceName == "" {
		log.Printf("dropping event: missing or invalid '_source' field in event data")
		return eventRecord{}, false, nil
	}

	source, exists := appConfig.Sources[sourceName]
	if !exists {
		log.Printf("dropping event: unknown source %q in configuration", sourceName)
		return eventRecord{}, false, nil
	}

	timestampField := source.TimestampField
	timestampLayout := source.TimestampFormat
	/*idField := source.IDField*/

	tsRaw, ok := fields[timestampField].(string)
	if !ok {
		log.Printf("dropping event: missing/non-string timestamp field %q", timestampField)
		return eventRecord{}, false, nil
	}

	t, err := time.Parse(timestampLayout, tsRaw)
	if err != nil {
		log.Printf("timestamp format error: %v", err)
		return eventRecord{}, false, nil
	}

	/*id, ok := fields[idField].(string)
	if ok && id != "" {
		tSec := t.Unix()
		tStr := strconv.FormatInt(tSec, 10)

		if err := rdb.SAdd(ctx, activeKey+":"+sourceName, id).Err(); err != nil {
			return fmt.Errorf("redis sadd active failed: %w", err)
		}

		if err := rdb.ZAdd(ctx, sessionKey+":"+sourceName+":"+id, redis.Z{
			Score:  float64(tSec),
			Member: tStr,
		}).Err(); err != nil {
			return fmt.Errorf("redis zadd session-time failed: %w", err)
		}
	}
	*/

	return eventRecord{
		key: dataKey + ":" + sourceName,
		z: redis.Z{
			Score:  float64(t.Unix()),
			Member: string(data),
		},
	}, true, nil
}

// processMessage parses a single event and writes it to Redis. Used by the
// push entry point (ingestEvent), which handles one event per invocation so
// there's nothing to batch. The pull entry point (ingestPull, pull.go) calls
// parseEvent directly and pipelines the resulting writes instead.
func processMessage(ctx context.Context, data []byte) error {
	rec, ok, err := parseEvent(data)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := rdb.ZAdd(ctx, rec.key, rec.z).Err(); err != nil {
		return fmt.Errorf("redis zadd failed: %w", err)
	}
	return nil
}
