package ingestor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/mardentub/ingestor/watermark"
)

const (
	watermarkKey = "watermark"

	// The windower's monotonic record of how far it has already closed windows.
	// Read-only here: the ingestor adopts it, the windower owns it.
	watermarkFloorKey = "watermark:floor"

	// Only bounds key lifetime; the windower judges freshness by published_at.
	watermarkHashTTL = 10 * time.Minute
)

// Instances must not overwrite each other: the windower takes their minimum.
var instanceID = newInstanceID()

func newInstanceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Hands a cold start the floor the windower already acted on, so its first
// sample holds instead of promising from a partial view. Without it, session
// churn under a backlog walks the watermark past undelivered events.
//
// Best effort: a missing floor is the normal first-run case.
func seedWatermarks(ctx context.Context, t *watermark.Tracker) {
	for source := range appConfig.Sources {
		floor, err := rdb.Get(ctx, watermarkFloorKey+":"+source).Int64()
		if err != nil {
			continue
		}
		t.Seed(source, floor)
		log.Printf("[Watermark] source=%s seeded from floor=%d", source, floor)
	}
}

// Writes this instance's watermark per source. Redis rather than the trigger
// body, so a tick the ingestor did not cause still sees it. Own context, like
// writeBatcher.flush: the last publish fires as the session deadline expires.
func publishWatermarks(t *watermark.Tracker, c watermark.Conditions) {
	samples := t.Watermarks(c)
	if len(samples) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().Unix()
	pipe := rdb.Pipeline()
	for source, sample := range samples {
		key := watermarkKey + ":" + source
		pipe.HSet(ctx, key, instanceID, fmt.Sprintf("%d:%d", sample.At, now))
		pipe.Expire(ctx, key, watermarkHashTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[Watermark] publish failed: %v", err)
		return
	}

	for source, sample := range samples {
		log.Printf("[Watermark] source=%s watermark=%d lag=%ds idle_snap=%t draining=%t late_arrivals=%d",
			source, sample.At, now-sample.At, sample.Snap, sample.Draining, sample.Late)
	}
}
