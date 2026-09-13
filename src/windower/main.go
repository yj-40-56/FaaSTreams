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
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/mardentub/windower/config"
	"github.com/redis/go-redis/v9"
)

// Redis key prefixes. Each is suffixed with the source name, and pending /
// inflight also carry the window (see pendingMember).
const (
	windowNextKey  = "window:next"
	pendingKey     = "pending"
	pendingMetaKey = "pending:meta"
	inflightKey    = "inflight"
	dataKey        = "data"
	lockKey        = "lock"
	sessionKey     = "session"
	activeKey      = "active"
)

// Timing knobs.
//
// These are absolute, which only holds while the window size stays in the same
// order of magnitude as they are. At a 60s window a 180s retry backstop is
// three windows; at a 5s window it is thirty-six, and a window would sit
// unprocessed far longer than it took to fill. Deriving them from
// q.WindowSize is the obvious next step.
const (
	// How long the windower waits on a worker before hanging up. The worker is
	// synchronous and takes minutes, so this expiring is normal and says
	// nothing about whether the window is being processed -- only the lease
	// does. See retryPending.
	workerTriggerTimeout = 30 * time.Second
	// Guards one query's window advance, one source's retry sweep, and one
	// session's rollup against an overlapping tick.
	lockTTL = 2 * time.Minute
	// Events arriving later than this are too late to join a closing window.
	lateBufferSeconds = 3
	// Backstop for a window whose trigger never reached a worker at all (a
	// 429), so no lease was ever taken. Long enough to cover a cold start,
	// no longer -- a leased window is skipped regardless of its age.
	pendingRetryAfterSeconds = 180
	// After this many attempts the window is given up on and dropped from
	// pending, which lets recordCleanup prune its events. The drop is logged.
	pendingMaxAttempts = 3
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

// cleanupBelowMin prunes events no window can still need. KEYS: window:next,
// data, pending. The boundary is the earliest of the next window each query
// will emit and the oldest window a worker has been handed but not yet
// confirmed -- a window that was emitted and never confirmed still owns its
// events, so it holds the boundary down until the worker clears it.
var cleanupBelowMin = redis.NewScript(`
local lo = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if lo[2] == nil then
    return {0, 0, 0, 0, 0}
end
local minScore = tonumber(lo[2])
local held = 0
local pend = redis.call('ZRANGE', KEYS[3], 0, 0, 'WITHSCORES')
if pend[2] ~= nil then
    local pendingMin = tonumber(pend[2])
    if pendingMin < minScore then
        minScore = pendingMin
        held = 1
    end
end
local minStr = string.format('%d', minScore)
local first = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', '(' .. minStr, 'WITHSCORES', 'LIMIT', 0, 1)
local last = redis.call('ZREVRANGEBYSCORE', KEYS[2], '(' .. minStr, '-inf', 'WITHSCORES', 'LIMIT', 0, 1)
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. minStr)
local minRemovedScore = 0
local maxRemovedScore = 0
if #first > 0 then
    minRemovedScore = tonumber(first[2])
    maxRemovedScore = tonumber(last[2])
end
return {minScore, removed, minRemovedScore, maxRemovedScore, held}
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
		coord.retryPending(ctx, sourceName)
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
	client := &http.Client{Timeout: workerTriggerTimeout}

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
		[]string{specificWindowNextKey, specificDataKey, pendingKey + ":" + prefix},
	).Result()
	if err != nil {
		log.Printf("[Cleanup] failed: %v", err)
		return
	}
	vals, ok := res.([]interface{})
	if !ok || len(vals) != 5 {
		return
	}
	minScore, removed, minRemoved, maxRemoved, held :=
		vals[0].(int64), vals[1].(int64), vals[2].(int64), vals[3].(int64), vals[4].(int64)
	if held == 1 {
		log.Printf("[Cleanup] source=%s boundary held at %d by an unconfirmed window", prefix, minScore)
	}
	if removed > 0 {
		log.Printf("[Cleanup] source=%s pruned %d event(s) below min_window_start=%d, removed_score_range=[%d,%d]",
			prefix, removed, minScore, minRemoved, maxRemoved)
	}
}

// pendingWindow is one window that has been emitted to a worker but not yet
// confirmed as processed.
type pendingWindow struct {
	start  int64
	end    int64
	member string
}

// pendingMember identifies a window inside pending:<source>. The worker
// rebuilds the same string from its own payload in order to clear the entry,
// so this format is a contract shared with src/worker/fetch.py:clear_pending.
func pendingMember(queryName, id string, start, end int64) string {
	return fmt.Sprintf("%s:%s:%d:%d", queryName, id, start, end)
}

// parsePendingMember splits from the right so a query name containing ':'
// survives the round trip.
func parsePendingMember(member string) (queryName, id string, start, end int64, ok bool) {
	parts := strings.Split(member, ":")
	if len(parts) < 4 {
		return "", "", 0, 0, false
	}
	start, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
	if err != nil {
		return "", "", 0, 0, false
	}
	end, err = strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return "", "", 0, 0, false
	}
	return strings.Join(parts[:len(parts)-3], ":"), parts[len(parts)-3], start, end, true
}

func formatPendingMeta(attempts int, lastAttempt int64) string {
	return fmt.Sprintf("%d:%d", attempts, lastAttempt)
}

func parsePendingMeta(raw string) (attempts int, lastAttempt int64) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return 1, 0
	}
	a, err := strconv.Atoi(parts[0])
	if err != nil {
		return 1, 0
	}
	t, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return a, 0
	}
	return a, t
}

// registerPending claims a window on behalf of the worker about to receive it.
func (c *Coordinator) registerPending(ctx context.Context, prefix, member string, winStart int64) error {
	pipe := c.rdb.TxPipeline()
	pipe.ZAdd(ctx, pendingKey+":"+prefix, redis.Z{Score: float64(winStart), Member: member})
	// NX: a re-emitted window must not reset its own attempt counter.
	pipe.HSetNX(ctx, pendingMetaKey+":"+prefix, member, formatPendingMeta(1, time.Now().Unix()))
	_, err := pipe.Exec(ctx)
	return err
}

// dropPending releases a window's claim without it having been processed. Its
// events become prunable, so every caller logs why.
func (c *Coordinator) dropPending(ctx context.Context, prefix, member string) {
	pipe := c.rdb.TxPipeline()
	pipe.ZRem(ctx, pendingKey+":"+prefix, member)
	pipe.HDel(ctx, pendingMetaKey+":"+prefix, member)
	pipe.Del(ctx, inflightKey+":"+prefix+":"+member)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[Pending] source=%s failed to drop %s: %v", prefix, member, err)
	}
}

func (c *Coordinator) queryByName(name string) (config.Query, bool) {
	for _, q := range c.config.Queries {
		if q.Name == name {
			return q, true
		}
	}
	return config.Query{}, false
}

// inflightMembers reports which of the given windows a worker is actively
// holding. The worker refreshes its lease while it runs (see
// src/worker/fetch.py:start_lease), so a missing key means no worker is on it
// -- either it never arrived, or the process died and the lease expired.
func (c *Coordinator) inflightMembers(ctx context.Context, prefix string, members []string) map[string]struct{} {
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(members))
	for i, member := range members {
		cmds[i] = pipe.Exists(ctx, inflightKey+":"+prefix+":"+member)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		// Assume everything is running rather than risk double-triggering a
		// live worker on a Redis hiccup.
		log.Printf("[Pending] source=%s lease check failed, skipping retries this tick: %v", prefix, err)
		running := make(map[string]struct{}, len(members))
		for _, member := range members {
			running[member] = struct{}{}
		}
		return running
	}
	running := make(map[string]struct{})
	for i, cmd := range cmds {
		if cmd.Val() > 0 {
			running[members[i]] = struct{}{}
		}
	}
	return running
}

// retryPending re-hands windows that were emitted but never confirmed. Every
// pending entry holds the whole source's retention boundary down, so entries
// that keep failing are eventually given up on rather than filling Redis.
func (c *Coordinator) retryPending(ctx context.Context, prefix string) {
	lockKeyPending := lockKey + ":" + pendingKey + ":" + prefix
	ok, _ := c.rdb.SetNX(ctx, lockKeyPending, "locked", lockTTL).Result()
	if !ok {
		return
	}
	defer c.rdb.Del(ctx, lockKeyPending)

	specificPendingKey := pendingKey + ":" + prefix
	specificMetaKey := pendingMetaKey + ":" + prefix

	members, err := c.rdb.ZRange(ctx, specificPendingKey, 0, -1).Result()
	if err != nil {
		log.Printf("[Pending] source=%s zrange failed: %v", prefix, err)
		return
	}
	if len(members) == 0 {
		// Nothing is in flight, so no metadata can still be live.
		c.rdb.Del(ctx, specificMetaKey)
		return
	}

	meta, err := c.rdb.HGetAll(ctx, specificMetaKey).Result()
	if err != nil {
		log.Printf("[Pending] source=%s hgetall failed: %v", prefix, err)
		return
	}

	inflight := c.inflightMembers(ctx, prefix, members)
	now := time.Now().Unix()
	var wg sync.WaitGroup

	for _, member := range members {
		if _, running := inflight[member]; running {
			// Not failed -- a worker is on it and saying so. Leave it alone;
			// it goes on holding the retention boundary, which is correct.
			continue
		}
		queryName, id, start, end, parsed := parsePendingMember(member)
		if !parsed {
			log.Printf("[Pending] source=%s dropping unparseable entry %q", prefix, member)
			c.dropPending(ctx, prefix, member)
			continue
		}
		q, found := c.queryByName(queryName)
		if !found {
			log.Printf("[Pending] source=%s dropping window %s: query %q is no longer configured",
				prefix, member, queryName)
			c.dropPending(ctx, prefix, member)
			continue
		}

		attempts, lastAttempt := parsePendingMeta(meta[member])
		if now-lastAttempt < pendingRetryAfterSeconds {
			// Still plausibly running: a worker holds a window for minutes.
			continue
		}
		if attempts >= pendingMaxAttempts {
			log.Printf("[Pending] source=%s GIVING UP on window %s after %d attempt(s); its events are now prunable",
				prefix, member, attempts)
			c.dropPending(ctx, prefix, member)
			continue
		}

		if err := c.rdb.HSet(ctx, specificMetaKey, member, formatPendingMeta(attempts+1, now)).Err(); err != nil {
			log.Printf("[Pending] source=%s failed to record retry of %s: %v", prefix, member, err)
			continue
		}
		log.Printf("[Pending] source=%s retrying window %s (attempt %d/%d)",
			prefix, member, attempts+1, pendingMaxAttempts)

		wg.Add(1)
		go func(q config.Query, id string, start, end int64, member string) {
			defer wg.Done()
			if err := c.triggerWorker(time.Unix(start, 0).UTC(), time.Unix(end, 0).UTC(), q, id); err != nil {
				log.Printf("[Pending] source=%s retry of %s failed: %v", prefix, member, err)
			}
		}(q, id, start, end, member)
	}

	wg.Wait()

	// Drop metadata for windows a worker has since confirmed.
	live := make(map[string]struct{}, len(members))
	for _, m := range members {
		live[m] = struct{}{}
	}
	var orphans []string
	for m := range meta {
		if _, stillPending := live[m]; !stillPending {
			orphans = append(orphans, m)
		}
	}
	if len(orphans) > 0 {
		c.rdb.HDel(ctx, specificMetaKey, orphans...)
	}
}

// tumbling: slideSecs == windowSec
func (c *Coordinator) createWindows(ctx context.Context, t time.Time, q config.Query, slideSecs int64) error {
	prefix := q.DataSource
	lockKeyWindow := lockKey + ":" + prefix + ":" + q.Name

	ok, _ := c.rdb.SetNX(ctx, lockKeyWindow, "locked", lockTTL).Result()
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

		var due []pendingWindow

		for tSec > endSec {
			winStart := endSec - windowSec
			count, _ := c.rdb.ZCount(ctx, specificDataKey,
				strconv.FormatInt(winStart, 10),
				"("+strconv.FormatInt(endSec, 10)).Result()
			if count > 0 {
				due = append(due, pendingWindow{
					start:  winStart,
					end:    endSec,
					member: pendingMember(q.Name, "", winStart, endSec),
				})
			} else {
				// Nothing to hand over, so nothing to claim: an empty window
				// must not leave a pending entry behind, or a quiet query
				// would pin the source's retention boundary forever.
				log.Printf("[Coordinator] EMPTY window query=%s source=%s range=[%d,%d) at check_time=%d",
					q.Name, prefix, winStart, endSec, tSec)
			}
			// tumbling: slideSecs == windowSec
			endSec += slideSecs
		}

		// Claim every window before triggering any of them. The pending entry
		// is what stops recordCleanup from pruning the window's events out
		// from under a worker that has not finished -- or never started.
		for _, w := range due {
			if err := c.registerPending(ctx, prefix, w.member, w.start); err != nil {
				// Leave the pointer where it is: advancing past a window we
				// could not claim is exactly the loss this is here to prevent.
				updateErr = fmt.Errorf("claim window [%d,%d) for %s failed: %w", w.start, w.end, q.Name, err)
				return
			}
		}

		var workerWg sync.WaitGroup
		for _, w := range due {
			workerWg.Add(1)
			go func(w pendingWindow) {
				defer workerWg.Done()
				if err := c.triggerWorker(time.Unix(w.start, 0).UTC(), time.Unix(w.end, 0).UTC(), q, ""); err != nil {
					// Deliberately still pending: the worker either never got
					// the window or never finished it, so its events must
					// survive and retryPending will hand it over again.
					log.Printf("[Coordinator] trigger worker %s [%d,%d) failed: %v", q.Name, w.start, w.end, err)
				}
			}(w)
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

		ok, _ := c.rdb.SetNX(ctx, lockKeySession, "locked", lockTTL).Result()
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
