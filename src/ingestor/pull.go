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
	"github.com/mardentub/ingestor/watermark"
	"github.com/redis/go-redis/v9"
)

const (
	drainIdleWindow = 2000 * time.Millisecond

	// Jobs on alternate minutes overlap, so no stretch goes unpulled.
	maxSessionDuration = 100 * time.Second

	// Proof of a backlog: fresh delivery is sub-second at the publish rate.
	backlogArrivalAge = 10 * time.Second

	// A backlog arrives interleaved with fresh messages, so one fresh moment
	// proves nothing. Measured: 2s leaked 6,830 events, 10s none.
	drainClearWindow = 10 * time.Second
	interval         = 5 * time.Second
	maxDelay         = 1500 * time.Millisecond

	// Delivered but not yet acked.
	pullMaxOutstandingMessages = 20000

	// Independent StreamingPull streams.
	pullNumGoroutines = 4

	// Max ZADDs accumulated before a batch is flushed as one Redis pipeline.
	pipelineBatchSize = 500

	// Keeps add() from blocking under normal load.
	pipelineChannelBuffer = 2000

	// Max time a partial batch waits before being flushed anyway.
	pipelineFlushInterval = 100 * time.Millisecond
)

var (
	pullSub     *pubsub.Subscriber
	windowerURL string

	// Instance-scoped: the watermark must not move backwards between sessions.
	tracker = watermark.New()
)

// Pairs a write with its message, so the ack follows the flush.
type batchItem struct {
	rec eventRecord
	msg *pubsub.Message
}

// Batches events on one goroutine, flushed as pipelined grouped ZADDs.
// One loop suffices: Exec latency is ~3-10ms, far more headroom than needed.
// add() is safe from any goroutine; close() once, after Receive returns.
type writeBatcher struct {
	items     chan batchItem
	done      chan struct{}
	tracker   *watermark.Tracker
	processed *int64
	failed    *int64

	// From the ZADD replies; diverges from processed when duplicates collapse.
	stored *int64
}

func newWriteBatcher(tracker *watermark.Tracker, processed, failed, stored *int64) *writeBatcher {
	b := &writeBatcher{
		items:     make(chan batchItem, pipelineChannelBuffer),
		done:      make(chan struct{}),
		tracker:   tracker,
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

// Once only, after every add() has returned.
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

// Acks/nacks each message by its group's result. Own context, not the
// session's: a flush past the deadline must still complete and ack.
func (b *writeBatcher) flush(batch []batchItem) {
	if len(batch) == 0 {
		return
	}

	// One ZADD per key, not per event. Pipelining cuts round-trips, but Redis
	// processes each pipelined command separately, and command count was the
	// measured ceiling on the single-threaded server.
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
			// Before the ack: in flight ends when the write resolves, not sooner.
			b.tracker.Done(it.rec.source, it.rec.ts, err == nil)
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
	// init() runs for every entry point in this dir, whichever --entry-point
	// selects. Absent vars mean this deployment is not the pull one: skip
	// registration rather than log.Fatal before PORT 8080 is bound.
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

// One Tick: a Receive session up to maxSessionDuration, writing via
// writeBatcher while a ticker nudges windower every interval. Exits early when
// quiet, so an idle pipeline costs nothing -- the backlog that then builds is
// safe because the watermark holds while old-published messages arrive.
// Sessions overlap by design: ZADDs are idempotent and windower takes the
// minimum watermark across instances.
func ingestPull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sessionCtx, cancelSession := context.WithTimeout(ctx, maxSessionDuration)
	defer cancelSession()

	sessionStart := time.Now()

	// Stops a cold start promising past an undelivered backlog. No-op when
	// warm: Seed only raises.
	seedWatermarks(sessionCtx, tracker)

	// isShuttingDown (0 = active, 1 = stopping) halts the ticker goroutine
	// once Receive returns.
	var processedSinceLastTick, failedSinceLastTick, storedSinceLastTick, isShuttingDown int64

	// Only an idle exit proves nothing was left to deliver, which is what
	// makes the no-allowance snap sound.
	var idleExit int64

	// Delivery age is the only local evidence of a backlog: event timestamps
	// say nothing about what Pub/Sub has not handed over yet.
	var lastBacklogArrivalAt atomic.Int64
	draining := func() bool {
		at := lastBacklogArrivalAt.Load()
		return at != 0 && time.Since(time.Unix(0, at)) < drainClearWindow
	}

	batcher := newWriteBatcher(tracker, &processedSinceLastTick, &failedSinceLastTick, &storedSinceLastTick)

	// Keeps the final windower trigger on the interval.
	var lastTickTime atomic.Value
	lastTickTime.Store(time.Now())

	// Triggers windower from inside the container, decoupled from the external
	// scheduler while data flows.
	windowerTicker := time.NewTicker(interval)
	defer windowerTicker.Stop()

	go func() {
		for {
			select {
			case <-windowerTicker.C:
				if atomic.LoadInt64(&isShuttingDown) == 1 {
					return
				}

				lastTickTime.Store(time.Now())

				currentProcessed := atomic.SwapInt64(&processedSinceLastTick, 0)
				currentFailed := atomic.SwapInt64(&failedSinceLastTick, 0)
				currentStored := atomic.SwapInt64(&storedSinceLastTick, 0)

				if currentFailed > 0 {
					log.Printf("[PullIngestor] %d messages failed", currentFailed)
				}

				// A window can span two intervals, so one interval's counts do
				// not decide whether to trigger.
				log.Printf("[PullIngestor] processed %d messages, stored %d (deduped %d)",
					currentProcessed, currentStored, currentProcessed-currentStored)
				publishWatermarks(tracker, watermark.Conditions{Draining: draining()})
				triggerWindower()

			case <-sessionCtx.Done():
				return
			}
		}
	}()

	log.Printf("[PullIngestor] starting continuous message consumption with internal %v ticker", interval)

	receiveCtx, cancelReceive := context.WithCancel(sessionCtx)
	idleTimer := time.AfterFunc(drainIdleWindow, func() {
		log.Printf("[PullIngestor] %v of absolute silence (queue empty). Terminating", drainIdleWindow)
		atomic.StoreInt64(&idleExit, 1)
		cancelReceive()
	})

	err := pullSub.Receive(receiveCtx, func(msgCtx context.Context, msg *pubsub.Message) {
		idleTimer.Reset(drainIdleWindow)
		if time.Since(msg.PublishTime) >= backlogArrivalAge {
			lastBacklogArrivalAt.Store(time.Now().UnixNano())
		}

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
		tracker.Begin(rec.source, rec.ts)
		batcher.add(batchItem{rec: rec, msg: msg})
	})

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("[PullIngestor] receive ended with error: %v", err)
	}

	atomic.StoreInt64(&isShuttingDown, 1)

	idleTimer.Stop()
	cancelReceive()

	// Flush before the final watermark, which must not promise past them.
	batcher.close()

	lastTick := lastTickTime.Load().(time.Time)
	timeSinceLastTick := time.Since(lastTick)

	// ensures that the Windower is triggered every `interval`
	if timeSinceLastTick < interval {
		remainingWait := interval - timeSinceLastTick
		log.Printf("[PullIngestor] Strict 5s pacing enforced. Sleeping for %v", remainingWait)
		time.Sleep(remainingWait)
	}

	absoluteDeadline := lastTick.Add(interval).Add(maxDelay)

	// After a long delay the next instance may already be triggering: drop
	// this one rather than duplicate a window.
	if time.Now().After(absoluteDeadline) {
		log.Printf("[PullIngestor] WARNING: Slept too long! Expected total ~%v, but actually passed %v. Dropping final trigger to prevent duplicate window.", interval, time.Since(lastTick))
		w.WriteHeader(http.StatusOK)
		return
	}

	finalProcessed := atomic.SwapInt64(&processedSinceLastTick, 0)
	finalStored := atomic.SwapInt64(&storedSinceLastTick, 0)

	log.Printf("[PullIngestor] processed %d messages, stored %d (deduped %d), session_elapsed=%s",
		finalProcessed, finalStored, finalProcessed-finalStored, time.Since(sessionStart))
	publishWatermarks(tracker, watermark.Conditions{
		Idle:     atomic.LoadInt64(&idleExit) == 1,
		Draining: draining(),
	})
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
