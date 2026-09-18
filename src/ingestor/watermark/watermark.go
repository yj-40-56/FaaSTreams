// Package watermark reports the oldest event that could still arrive.
package watermark

import "sync"

// Covers what the in-flight minimum cannot: an undelivered message could still
// carry an older timestamp. Size it from the late counts, don't guess.
const AllowedLatenessSeconds = 5

// Tracker promises, per source, that no event older than the watermark is still
// unwritten. In flight means parsed but not yet acked.
type Tracker struct {
	mu      sync.Mutex
	sources map[string]*sourceState
}

type sourceState struct {
	inflight   map[int64]int // event second -> messages received but not yet written
	maxWritten int64
	published  int64
	late       int64
}

type Sample struct {
	At       int64
	Late     int64
	Snap     bool
	Draining bool
}

// Conditions are what the ingestor observed about delivery itself, which the
// in-flight set cannot show.
type Conditions struct {
	// Idle: nothing delivered for a while, so nothing is outstanding and the
	// lateness allowance can be dropped.
	Idle bool

	// Draining: long-published messages are still arriving, so a backlog
	// remains. Pub/Sub delivers one out of order, so anything undelivered may
	// predate everything in flight: hold the promise until delivery is fresh.
	Draining bool
}

func New() *Tracker {
	return &Tracker{sources: make(map[string]*sourceState)}
}

func (t *Tracker) source(name string) *sourceState {
	s, ok := t.sources[name]
	if !ok {
		s = &sourceState{inflight: make(map[int64]int)}
		t.sources[name] = s
	}
	return s
}

// Adopts a promise made before this instance existed. Without it a cold start
// has published == 0, so the Draining hold cannot engage and the instance
// promises from the few messages it drained -- ahead of what is undelivered.
func (t *Tracker) Seed(source string, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.source(source)
	if ts > s.published {
		s.published = ts
	}
}

func (t *Tracker) Begin(source string, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.source(source)
	if s.published > 0 && ts < s.published {
		s.late++
	}
	s.inflight[ts]++
}

// Done releases a message. A nacked one is released too: holding it would pin
// the watermark on a redelivery that may go to another instance.
func (t *Tracker) Done(source string, ts int64, written bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.source(source)
	if n := s.inflight[ts]; n > 1 {
		s.inflight[ts] = n - 1
	} else {
		delete(s.inflight, ts)
	}
	if written && ts > s.maxWritten {
		s.maxWritten = ts
	}
}

func (s *sourceState) oldestInflight() (int64, bool) {
	oldest := int64(0)
	for ts := range s.inflight {
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	return oldest, oldest != 0
}

// Watermarks computes each source's watermark and records it as published,
// under what the ingestor observed about delivery.
func (t *Tracker) Watermarks(c Conditions) map[string]Sample {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make(map[string]Sample, len(t.sources))
	for name, s := range t.sources {
		oldest, inflight := s.oldestInflight()
		var wm int64
		snap := false
		switch {
		case c.Draining && s.published > 0:
			wm = s.published
		case inflight:
			wm = oldest - AllowedLatenessSeconds
		case s.maxWritten == 0:
			continue
		case c.Idle:
			wm = s.maxWritten
			snap = true
		default:
			wm = s.maxWritten - AllowedLatenessSeconds
		}
		// A watermark that moved backwards would break its own promise.
		if wm < s.published {
			wm = s.published
		}
		if wm <= 0 {
			continue
		}
		s.published = wm
		out[name] = Sample{At: wm, Late: s.late, Snap: snap, Draining: c.Draining}
		s.late = 0
	}
	return out
}
