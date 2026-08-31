package watermark

import "testing"

const src = "ais_data_v1"

func TestWatermarkHeldByOldestInFlight(t *testing.T) {
	tr := New()
	tr.Begin(src, 100)
	tr.Begin(src, 200)
	tr.Done(src, 200, true)

	got := tr.Watermarks(false)[src].At
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

	got := tr.Watermarks(false)[src].At
	if want := int64(200 - AllowedLatenessSeconds); got != want {
		t.Fatalf("watermark = %d, want %d", got, want)
	}
}

func TestIdleSnapDropsTheLatenessAllowance(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)

	sample := tr.Watermarks(true)[src]
	if sample.At != 200 || !sample.Snap {
		t.Fatalf("idle snap = %d (snap=%t), want 200 (snap=true)", sample.At, sample.Snap)
	}
}

func TestWatermarkNeverMovesBackwards(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)
	tr.Watermarks(true)

	tr.Begin(src, 100)
	if got := tr.Watermarks(false)[src].At; got != 200 {
		t.Fatalf("watermark = %d, want it held at 200 despite the older arrival", got)
	}
}

func TestLateArrivalsAreCountedAndReset(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, true)
	tr.Watermarks(true)

	tr.Begin(src, 100)
	if got := tr.Watermarks(false)[src].Late; got != 1 {
		t.Fatalf("late = %d, want 1", got)
	}
	if got := tr.Watermarks(false)[src].Late; got != 0 {
		t.Fatalf("late = %d after publishing, want the counter reset", got)
	}
}

func TestUnwrittenSourceHasNoWatermark(t *testing.T) {
	tr := New()
	tr.Begin(src, 200)
	tr.Done(src, 200, false)

	if _, ok := tr.Watermarks(false)[src]; ok {
		t.Fatal("a source whose only write failed must not publish a watermark")
	}
}

func TestSourcesAreIndependent(t *testing.T) {
	tr := New()
	tr.Begin("a", 100)
	tr.Begin("b", 500)
	tr.Done("b", 500, true)

	if got := tr.Watermarks(false)["a"].At; got != 100-AllowedLatenessSeconds {
		t.Fatalf("source a watermark = %d, want %d: its in-flight event bounds it", got, 100-AllowedLatenessSeconds)
	}
	if got := tr.Watermarks(false)["b"].At; got != 500-AllowedLatenessSeconds {
		t.Fatalf("source b watermark = %d, want %d", got, 500-AllowedLatenessSeconds)
	}
}
