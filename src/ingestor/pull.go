package ingestor

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/redis/go-redis/v9"
)

const (
	pullLockKey        = "lock:ingestor:pull"
	pullLockTTL        = 7 * time.Second
	drainIdleWindow    = 2000 * time.Millisecond
	maxSessionDuration = 50 * time.Second
	interval           = 5 * time.Second
	maxDelay           = 1500 * time.Millisecond

	// how many messages can be outstanding at once — meaning delivered by the server to this client but not yet acked
	pullMaxOutstandingMessages = 20000

	// number of independent StreamingPull streams the client opens to the subscription
	pullNumGoroutines = 4

	// Max ZADDs accumulated before a batch is flushed as one Redis pipeline.
	pipelineBatchSize = 500

	// Buffer size of the channel feeding the flush loop, so add() doesn't block under normal load.
	pipelineChannelBuffer = 2000

	// Max time a partial batch waits before being flushed anyway.
	pipelineFlushInterval = 100 * time.Millisecond
)

var (
	pullSub     *pubsub.Subscriber
	windowerURL string
)

// batchItem pairs a parsed Redis write with the pubsub message it came from,
// so the message can be acked/nacked once the batch it landed in is flushed.
type batchItem struct {
	rec eventRecord
	msg *pubsub.Message
}

// writeBatcher accumulates parsed events on a single background goroutine
// and flushes them to Redis as pipelined, grouped ZADDs. A single loop is
// enough — measured Exec latency is only ~3-10ms, so it can push far more
// throughput than this pipeline needs without needing concurrent flushers.
// Call add() from any goroutine; call close() once (after Receive returns)
// to flush whatever's left and wait for the loop to finish.
type writeBatcher struct {
	items     chan batchItem
	done      chan struct{}
	processed *int64
	failed    *int64

	// Members actually added, summed from the ZADD replies. Diverges from
	// processed when duplicate payloads collapse.
	stored *int64
}

func newWriteBatcher(processed, failed, stored *int64) *writeBatcher {
	b := &writeBatcher{
		items:     make(chan batchItem, pipelineChannelBuffer),
		done:      make(chan struct{}),
		processed: processed,
		failed:    failed,
		stored:    stored,
	}
	go b.run()
	return b
}

func (b *writeBatcher) add(item batchItem) {
	b.items <- item
}

// close stops accepting new items, flushes whatever's buffered, and waits
// for the loop to drain. Must only be called once, after all add() calls
// have returned.
func (b *writeBatcher) close() {
	close(b.items)
	<-b.done
}

func (b *writeBatcher) run() {
	defer close(b.done)

	batch := make([]batchItem, 0, pipelineBatchSize)
	ticker := time.NewTicker(pipelineFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case item, ok := <-b.items:
			if !ok {
				b.flush(batch)
				return
			}
			batch = append(batch, item)
			if len(batch) >= pipelineBatchSize {
				b.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// flush pipelines batch to Redis and acks/nacks each message by its group's
// result. It uses its own context rather than the session's context, since a
// flush triggered near/after the session deadline must still be able to
// complete and ack — it isn't part of the pull itself, just bookkeeping for
// messages already received.
func (b *writeBatcher) flush(batch []batchItem) {
	if len(batch) == 0 {
		return
	}

	// Group by target key so events bound for the same sorted set collapse
	// into a single ZADD with many score/member pairs, instead of one ZADD
	// per event. Pipelining already cut round-trips; Redis still counts and
	// processes each pipelined command separately, so this is what actually
	// cuts the number of commands the (single-threaded) server has to work
	// through — the thing INFO stats showed was the real ceiling.
	groups := make(map[string][]batchItem, 1)
	for _, it := range batch {
		groups[it.rec.key] = append(groups[it.rec.key], it)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pipe := rdb.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(groups))
	for key, items := range groups {
		members := make([]redis.Z, len(items))
		for i, it := range items {
			members[i] = it.rec.z
		}
		cmds[key] = pipe.ZAdd(ctx, key, members...)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("[PullIngestor] pipeline exec error (batch=%d groups=%d): %v", len(batch), len(groups), err)
	}

	for key, items := range groups {
		err := cmds[key].Err()
		if err == nil {
			atomic.AddInt64(b.stored, cmds[key].Val())
		}
		for _, it := range items {
			if err != nil {
				log.Printf("[PullIngestor] nacking message: redis zadd failed: %v", err)
				it.msg.Nack()
				atomic.AddInt64(b.failed, 1)
				continue
			}
			it.msg.Ack()
			atomic.AddInt64(b.processed, 1)
		}
	}
}

func init() {
	// init() runs unconditionally for every entry point built from this source
	// dir (main.go's IngestEvent included) regardless of which one --entry-point
	// actually selects at deploy time. The push ingestor's env file doesn't set
	// these, so treat their absence as "this deployment isn't using the pull
	// entry point" and skip registration, rather than log.Fatal-ing the whole
	// process before it can bind PORT 8080.
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	subID := os.Getenv("PUBSUB_PULL_SUBSCRIPTION_ID")
	windowerURL = os.Getenv("WINDOWER_URL")
	if projectID == "" || subID == "" || windowerURL == "" {
		return
	}

	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("pubsub client init failed: %v", err)
	}
	pullSub = client.Subscriber(subID)
	pullSub.ReceiveSettings.MaxOutstandingMessages = pullMaxOutstandingMessages
	pullSub.ReceiveSettings.NumGoroutines = pullNumGoroutines

	functions.HTTP("IngestPull", ingestPull)
}

// ingestPull is a Tick: it keeps a single Pub/Sub Receive session open for up
// to maxSessionDuration, parsing and pipelining writes to Redis (via
// writeBatcher) as messages arrive, while an internal ticker nudges windower
// and renews the Redis lock every `interval`. The session ends early if the
// subscription goes idle for drainIdleWindow. A Redis SetNX lock guards
// against overlapping ticks if a session runs long.
func ingestPull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	locked, err := rdb.SetNX(ctx, pullLockKey, "locked", pullLockTTL).Result()
	if err != nil {
		http.Error(w, "lock check failed", http.StatusInternalServerError)
		log.Printf("[PullIngestor] lock check failed: %v", err)
		return
	}
	if !locked {
		log.Printf("[PullIngestor] previous tick still draining, skipping this tick")
		w.WriteHeader(http.StatusOK)
		return
	}

	// CRITICAL: We only delete the lock here if the normal flow didn't release it yet
	lockReleased := false
	defer func() {
		if !lockReleased {
			rdb.Del(context.Background(), pullLockKey)
		}
	}()

	sessionCtx, cancelSession := context.WithTimeout(ctx, maxSessionDuration)
	defer cancelSession()

	sessionStart := time.Now()

	// `isShuttingDown` acts as a thread-safe coordination barrier (0 = active, 1 = stopping).
	// It ensures the background ticker goroutine completely halts its triggers once the
	// SubPub-Pulling terminates
	var processedSinceLastTick, failedSinceLastTick, storedSinceLastTick, isShuttingDown int64

	batcher := newWriteBatcher(&processedSinceLastTick, &failedSinceLastTick, &storedSinceLastTick)

	// to ensure that the last Windower trigger strictly adheres to the interval of `interval` (5s) seconds
	var lastTickTime atomic.Value
	lastTickTime.Store(time.Now())

	// autonomous 5-second windower ticker
	// internal ticker to guarantee that the Windower is triggered exactly
	// every `interval` (5s) seconds from within this container, completely decoupling from the
	// external scheduler as long as data is flowing.
	windowerTicker := time.NewTicker(interval)
	defer windowerTicker.Stop()

	// triggering the windower every `interval` seconds,
	// until the maximum session duration (`maxSessionDuration`) is reached.
	go func() {
		for {
			select {
			case <-windowerTicker.C:
				if atomic.LoadInt64(&isShuttingDown) == 1 {
					return
				}

				lastTickTime.Store(time.Now())

				rdb.Expire(sessionCtx, pullLockKey, pullLockTTL)

				currentProcessed := atomic.SwapInt64(&processedSinceLastTick, 0)
				currentFailed := atomic.SwapInt64(&failedSinceLastTick, 0)
				currentStored := atomic.SwapInt64(&storedSinceLastTick, 0)

				if currentFailed > 0 {
					log.Printf("[PullIngestor] %d messages failed", currentFailed)
				}

				// There could be cases where a window is 2xinterval (10s) long,
				// in which case the last `interval` (5s) are not sufficient to determine whether the window should be triggered
				log.Printf("[PullIngestor] processed %d messages, stored %d (deduped %d)",
					currentProcessed, currentStored, currentProcessed-currentStored)
				triggerWindower()

			case <-sessionCtx.Done():
				return
			}
		}
	}()

	// Idle Guardian
	// creates a child context for message pulling.
	// If the queue goes empty and no messages arrive within `drainIdleWindow`,
	// it triggers a background Goroutine to call cancelReceive().
	// This breaks pullSub.Receive immediately
	receiveCtx, cancelReceive := context.WithCancel(sessionCtx)
	idleTimer := time.AfterFunc(drainIdleWindow, func() {
		log.Printf("[PullIngestor] %v of absolute silence (queue empty). Terminating", drainIdleWindow)
		cancelReceive()
	})

	log.Printf("[PullIngestor] starting continuous message consumption with internal %v ticker", interval)

	err = pullSub.Receive(receiveCtx, func(msgCtx context.Context, msg *pubsub.Message) {
		idleTimer.Reset(drainIdleWindow)

		rec, ok, procErr := parseEvent(msg.Data)
		if procErr != nil {
			log.Printf("[PullIngestor] nacking message: %v", procErr)
			msg.Nack()
			atomic.AddInt64(&failedSinceLastTick, 1)
			return
		}
		if !ok {
			// Malformed/unroutable: ack directly, nothing to write.
			msg.Ack()
			atomic.AddInt64(&processedSinceLastTick, 1)
			return
		}
		batcher.add(batchItem{rec: rec, msg: msg})
	})

	atomic.StoreInt64(&isShuttingDown, 1)

	idleTimer.Stop()
	cancelReceive()

	// Receive only returns once nothing is left to deliver, but any partial
	// batches are still sitting in the batcher's buffer — flush and wait for
	// them before releasing the lock and triggering the final windower call,
	// so the next container instance's writes can't race unflushed data from
	// this one for the same window.
	batcher.close()

	// Release the Redis lock early. Since the PubSub-Pulling
	// has already terminated, this instance will not fetch any more data.
	// Releasing `pullLockKey` now allows the next scheduled container instance
	// to start working immediately without blockages.
	// We explicitly set `lockReleased = true`. This prevents the
	// top-level `defer` function from executing a second `rdb.Del`.
	rdb.Del(context.Background(), pullLockKey)
	lockReleased = true

	lastTick := lastTickTime.Load().(time.Time)
	timeSinceLastTick := time.Since(lastTick)

	// ensures that the Windower is triggered every `interval`
	if timeSinceLastTick < interval {
		remainingWait := interval - timeSinceLastTick
		log.Printf("[PullIngestor] Strict 5s pacing enforced. Sleeping for %v", remainingWait)
		time.Sleep(remainingWait)
	}

	absoluteDeadline := lastTick.Add(interval).Add(maxDelay)

	// If this goroutine suffered from a massive delay
	// the next container instance might already be active and triggering Windowers.
	// Therefore, this final window trigger is dropped to prevent duplicates.
	if time.Now().After(absoluteDeadline) {
		log.Printf("[PullIngestor] WARNING: Slept too long! Expected total ~%v, but actually passed %v. Dropping final trigger to prevent duplicate window.", interval, time.Since(lastTick))
		w.WriteHeader(http.StatusOK)
		return
	}

	finalProcessed := atomic.SwapInt64(&processedSinceLastTick, 0)
	finalStored := atomic.SwapInt64(&storedSinceLastTick, 0)

	log.Printf("[PullIngestor] processed %d messages, stored %d (deduped %d), session_elapsed=%s",
		finalProcessed, finalStored, finalProcessed-finalStored, time.Since(sessionStart))
	triggerWindower()

	w.WriteHeader(http.StatusOK)
}

func triggerWindower() {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(windowerURL, "application/json", nil)
	if err != nil {
		log.Printf("[PullIngestor] windower trigger failed: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("[PullIngestor] windower trigger returned status %d", resp.StatusCode)
	}
}
