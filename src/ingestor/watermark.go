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

// publishWatermarks writes this instance's watermark per source. Redis rather
// than the windower trigger's body, so a tick the ingestor did not cause sees it.
//
// Own context, like writeBatcher.flush: a session's last publish fires as the
// session deadline expires.
func publishWatermarks(t *watermark.Tracker, idle bool) {
	samples := t.Watermarks(idle)
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
		log.Printf("[Watermark] source=%s watermark=%d lag=%ds idle_snap=%t late_arrivals=%d",
			source, sample.At, now-sample.At, sample.Snap, sample.Late)
	}
}
