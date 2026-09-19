package watermark

import "testing"

const src = "ais_data_v1"

func TestWatermarkHeldByOldestInFlight(t *testing.T) {
	tr := New()
	tr.Begin(src, 100)
	tr.Begin(src, 200)
	tr.Done(src, 200, true)

	got := tr.Watermarks(Conditions{})[src].At
	if want := int64(100 - AllowedLatenessSeconds); got != want {
		t.Fatalf("watermark = %d, want %d: the unwritten event at 100 must hold it", got, want)
	}
}

func TestWatermarkAdvancesOnceNothingIsInFlight(t *testing.T) {
	tr := New()
	tr.Begin(src, 100)
	tr.Begin(src, 200)
	tr.Done(src, 100, true)
	tr.Done(src, 200, true)

	got := tr.Watermarks(Conditions{})[src].At
	if want := int64(200 - AllowedLatenessSeconds); got != want {
		t.Fatalf("watermark = %d, want %d", got, want)
	}
}

func TestIdleSnapDropsTheLatenessAllowance(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)

	sample := tr.Watermarks(Conditions{Idle: true})[src]
	if sample.At != 200 || !sample.Snap {
		t.Fatalf("idle snap = %d (snap=%t), want 200 (snap=true)", sample.At, sample.Snap)
	}
}

func TestWatermarkNeverMovesBackwards(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)
	tr.Watermarks(Conditions{Idle: true})

	tr.Begin(src, 100)
	if got := tr.Watermarks(Conditions{})[src].At; got != 200 {
		t.Fatalf("watermark = %d, want it held at 200 despite the older arrival", got)
	}
}

func TestLateArrivalsAreCountedAndReset(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)
	tr.Watermarks(Conditions{Idle: true})

	tr.Begin(src, 100)
	if got := tr.Watermarks(Conditions{})[src].Late; got != 1 {
		t.Fatalf("late = %d, want 1", got)
	}
	if got := tr.Watermarks(Conditions{})[src].Late; got != 0 {
		t.Fatalf("late = %d after publishing, want the counter reset", got)
	}
}

func TestDrainingHoldsTheWatermarkWhereItWas(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)
	held := tr.Watermarks(Conditions{})[src].At

	// Delivery has run ahead while a backlog is still being drained: the
	// undelivered part may be older than anything in flight.
	tr.Begin(src, 400)

	if got := tr.Watermarks(Conditions{Draining: true})[src].At; got != held {
		t.Fatalf("watermark = %d, want it held at %d while draining", got, held)
	}
	if got := tr.Watermarks(Conditions{})[src].At; got != 400-AllowedLatenessSeconds {
		t.Fatalf("watermark = %d, want %d once delivery is fresh", got, 400-AllowedLatenessSeconds)
	}
}

func TestUnwrittenSourceHasNoWatermark(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, false)

	if _, ok := tr.Watermarks(Conditions{})[src]; ok {
		t.Fatal("a source whose only write failed must not publish a watermark")
	}
}

func TestSourcesAreIndependent(t *testing.T) {
	tr := New()
	tr.Begin("a", 100)
	tr.Begin("b", 500)
	tr.Done("b", 500, true)

	if got := tr.Watermarks(Conditions{})["a"].At; got != 100-AllowedLatenessSeconds {
		t.Fatalf("source a watermark = %d, want %d: its in-flight event bounds it", got, 100-AllowedLatenessSeconds)
	}
	if got := tr.Watermarks(Conditions{})["b"].At; got != 500-AllowedLatenessSeconds {
		t.Fatalf("source b watermark = %d, want %d", got, 500-AllowedLatenessSeconds)
	}
}

// A cold start under a backlog: published is 0, so the Draining hold cannot
// engage and the instance publishes a watermark built from the few messages it
// happened to drain.
func TestColdStartWhileDrainingRunsAhead(t *testing.T) {
	tr := New()
	tr.Begin(src, 400)

	if got := tr.Watermarks(Conditions{Draining: true})[src].At; got != 400-AllowedLatenessSeconds {
		t.Fatalf("watermark = %d, want %d: without a seed there is no promise to hold", got, 400-AllowedLatenessSeconds)
	}
}

func TestSeedLetsAColdStartHoldTheFloor(t *testing.T) {
	const floor = 200

	tr := New()
	tr.Seed(src, floor)

	// Same partial view as above, but the promise is now inherited.
	tr.Begin(src, 400)

	if got := tr.Watermarks(Conditions{Draining: true})[src].At; got != floor {
		t.Fatalf("watermark = %d, want it held at the seeded floor %d", got, floor)
	}
	if got := tr.Watermarks(Conditions{})[src].At; got != 400-AllowedLatenessSeconds {
		t.Fatalf("watermark = %d, want %d once delivery is fresh", got, 400-AllowedLatenessSeconds)
	}
}

func TestSeedNeverLowersThePromise(t *testing.T) {
	tr := New()
	tr.Begin(src, 400)
	tr.Done(src, 400, true)
	promised := tr.Watermarks(Conditions{})[src].At

	tr.Seed(src, promised-100)

	if got := tr.Watermarks(Conditions{Draining: true})[src].At; got != promised {
		t.Fatalf("watermark = %d, want %d: a stale floor must not walk it backwards", got, promised)
	}
}
