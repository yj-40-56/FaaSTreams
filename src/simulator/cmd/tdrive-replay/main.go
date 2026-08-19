package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
)

type workloadRow struct {
	taxiID string
	offset float64
	lon    string
	lat    string
}

type publishResult struct {
	scheduled float64
	sent      float64
	acked     float64
	latencyMS float64
	ok        bool
	err       string
}

func main() {
	var (
		projectID         = flag.String("project", "faastreams", "Google Cloud project ID")
		topicID           = flag.String("topic", "ais-stream", "Pub/Sub topic used by the ingestor")
		sourceName        = flag.String("source", "tdrive_data_v1", "_source value configured for ingestor/windower")
		inputPath         = flag.String("input", "../../data/tdrive_workload_volatile.csv", "prepared T-Drive workload CSV")
		resultsPath       = flag.String("results", "../../results_tdrive_replay.csv", "publish-integrity CSV")
		triggerURL        = flag.String("trigger-url", "", "HTTP URL to POST to on an interval - windower's ProcessWindows under push, or ingestor-pull's IngestPull under pull (required unless --dry-run or --poll-trigger=false)")
		enableTriggerPoll = flag.Bool("poll-trigger", true, "poll --trigger-url on an interval. Under the pull ingestor, point --trigger-url at ingestor-pull instead of windower - each tick drains the subscription and triggers ProcessWindows itself")
		triggerEvery      = flag.Duration("trigger-every", time.Second, "interval between --trigger-url calls")
		drainTime         = flag.Duration("drain-time", 70*time.Second, "time to keep invoking --trigger-url after the final event")
		runDuration       = flag.Float64("run-duration", 0, "total replay duration in seconds, including a final quiet period (default: last event offset)")
		dryRun            = flag.Bool("dry-run", false, "validate the complete workload without contacting Google Cloud")
	)
	flag.Parse()

	rows, err := loadWorkload(*inputPath)
	if err != nil {
		log.Fatal(err)
	}
	lastEventOffset := rows[len(rows)-1].offset
	duration := lastEventOffset
	if *runDuration > 0 {
		duration = *runDuration
	}
	if duration < lastEventOffset {
		log.Fatalf("--run-duration %.3fs is before the final event at %.3fs", duration, lastEventOffset)
	}
	log.Printf("validated %d T-Drive events; last event %.3fs, total run %.3fs, final quiet period %.3fs (peak=%d events/s)",
		len(rows), lastEventOffset, duration, duration-lastEventOffset, peakRate(rows))
	if *dryRun {
		log.Printf("dry-run complete; no messages sent and no HTTP endpoints called")
		return
	}
	if *enableTriggerPoll && strings.TrimSpace(*triggerURL) == "" {
		log.Fatal("--trigger-url is required unless --poll-trigger=false: without it the replay stops at Redis and is not an end-to-end test")
	}
	if *triggerEvery <= 0 || *drainTime < 0 {
		log.Fatal("--trigger-every must be positive and --drain-time cannot be negative")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("create Pub/Sub client: %v", err)
	}
	defer client.Close()
	topic := client.Topic(*topicID)
	defer topic.Stop()
	topic.PublishSettings.DelayThreshold = 50 * time.Millisecond
	topic.PublishSettings.CountThreshold = 500
	topic.PublishSettings.ByteThreshold = 1_000_000
	topic.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
		MaxOutstandingMessages: 5000,
		MaxOutstandingBytes:    256 << 20,
		LimitExceededBehavior:  pubsub.FlowControlBlock,
	}

	resultCh := make(chan publishResult, 8192)
	writerDone := make(chan error, 1)
	go func() { writerDone <- writeResults(*resultsPath, resultCh) }()

	start := time.Now()
	pollerDone := make(chan error, 1)
	if *enableTriggerPoll {
		go func() {
			// Poll only through the final data window and its drain allowance. The
			// remaining train-day shutdown must be genuinely idle so the target
			// (windower or ingestor-pull) can scale down along with the other
			// pipeline stages.
			pollerDone <- pollTrigger(ctx, *triggerURL, *triggerEvery, start.Add(seconds(lastEventOffset)).Add(*drainTime))
		}()
	} else {
		log.Printf("--poll-trigger=false: not polling --trigger-url")
		pollerDone <- nil
	}

	var acknowledgements sync.WaitGroup
	var ackFailures atomic.Int64
	sendLags := make([]float64, 0, len(rows))
	for _, row := range rows {
		due := start.Add(seconds(row.offset))
		if wait := time.Until(due); wait > 0 {
			time.Sleep(wait)
		}
		sentAt := time.Now()
		sendLags = append(sendLags, sentAt.Sub(start).Seconds()-row.offset)
		payload, err := json.Marshal(map[string]string{
			"_source": *sourceName,
			"taxi_id": row.taxiID,
			"ts":      due.UTC().Format(time.RFC3339Nano),
			"lon":     row.lon,
			"lat":     row.lat,
		})
		if err != nil {
			log.Fatalf("encode taxi %q: %v", row.taxiID, err)
		}
		future := topic.Publish(ctx, &pubsub.Message{Data: payload})
		acknowledgements.Add(1)
		go func(scheduled, sent float64, sentAt time.Time, result *pubsub.PublishResult) {
			defer acknowledgements.Done()
			_, publishErr := result.Get(ctx)
			ackedAt := time.Now()
			entry := publishResult{
				scheduled: scheduled,
				sent:      sent,
				acked:     ackedAt.Sub(start).Seconds(),
				latencyMS: ackedAt.Sub(sentAt).Seconds() * 1000,
				ok:        publishErr == nil,
			}
			if publishErr != nil {
				ackFailures.Add(1)
				entry.err = publishErr.Error()
			}
			resultCh <- entry
		}(row.offset, sentAt.Sub(start).Seconds(), sentAt, future)
	}

	topic.Flush()
	acknowledgements.Wait()
	close(resultCh)
	if err := <-writerDone; err != nil {
		log.Fatalf("write results: %v", err)
	}
	if err := <-pollerDone; err != nil {
		log.Fatalf("trigger integrity failure: %v", err)
	}
	if wait := time.Until(start.Add(seconds(duration))); wait > 0 {
		log.Printf("final shutdown: waiting %.1f minutes with no publishes or trigger calls", wait.Minutes())
		time.Sleep(wait)
	}
	sort.Float64s(sendLags)
	p95Index := int(0.95 * float64(len(sendLags)-1))
	log.Printf("client send lag: p95=%.3fs max=%.3fs", sendLags[p95Index], sendLags[len(sendLags)-1])
	if ackFailures.Load() > 0 {
		log.Fatalf("publish integrity failure: %d/%d messages were not acknowledged", ackFailures.Load(), len(rows))
	}
	log.Printf("replay complete; publish results written to %s", *resultsPath)
}

func loadWorkload(path string) ([]workloadRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open workload: %w", err)
	}
	defer file.Close()
	reader := csv.NewReader(bufio.NewReaderSize(file, 1<<20))
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	want := []string{"taxi_id", "emit_offset_s", "lon", "lat"}
	if len(header) != len(want) {
		return nil, fmt.Errorf("wrong columns: got %v, want %v", header, want)
	}
	for i := range want {
		if header[i] != want[i] {
			return nil, fmt.Errorf("wrong column %d: got %q, want %q", i+1, header[i], want[i])
		}
	}
	rows := make([]workloadRow, 0, 1_000_000)
	previous := -1.0
	for line := 2; ; line++ {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("line %d: %w", line, readErr)
		}
		offset, parseErr := strconv.ParseFloat(record[1], 64)
		if parseErr != nil || offset < 0 || offset < previous {
			return nil, fmt.Errorf("line %d: invalid/non-monotonic emit_offset_s %q", line, record[1])
		}
		if strings.TrimSpace(record[0]) == "" {
			return nil, fmt.Errorf("line %d: empty taxi_id", line)
		}
		if _, parseErr = strconv.ParseFloat(record[2], 64); parseErr != nil {
			return nil, fmt.Errorf("line %d: invalid longitude %q", line, record[2])
		}
		if _, parseErr = strconv.ParseFloat(record[3], 64); parseErr != nil {
			return nil, fmt.Errorf("line %d: invalid latitude %q", line, record[3])
		}
		rows = append(rows, workloadRow{taxiID: record[0], offset: offset, lon: record[2], lat: record[3]})
		previous = offset
	}
	if len(rows) == 0 {
		return nil, errors.New("workload contains no events")
	}
	return rows, nil
}

func peakRate(rows []workloadRow) int {
	counts := make(map[int]int)
	peak := 0
	for _, row := range rows {
		bucket := int(row.offset)
		counts[bucket]++
		if counts[bucket] > peak {
			peak = counts[bucket]
		}
	}
	return peak
}

func writeResults(path string, results <-chan publishResult) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	buffer := bufio.NewWriterSize(file, 1<<20)
	writer := csv.NewWriter(buffer)
	if err := writer.Write([]string{"scheduled_s", "send_s", "publish_ack_s", "publish_latency_ms", "acked", "error"}); err != nil {
		return err
	}
	for result := range results {
		if err := writer.Write([]string{
			fmt.Sprintf("%.6f", result.scheduled), fmt.Sprintf("%.6f", result.sent),
			fmt.Sprintf("%.6f", result.acked), fmt.Sprintf("%.3f", result.latencyMS),
			strconv.FormatBool(result.ok), result.err,
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	return buffer.Flush()
}

func pollTrigger(ctx context.Context, url string, every time.Duration, until time.Time) error {
	client := &http.Client{Timeout: 30 * time.Second}
	var calls, failures atomic.Int64
	for time.Now().Before(until) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		calls.Add(1)
		if err != nil {
			failures.Add(1)
			log.Printf("trigger call failed: %v", err)
		} else {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode >= 300 {
				failures.Add(1)
				log.Printf("trigger call returned HTTP %d", response.StatusCode)
			}
		}
		time.Sleep(every)
	}
	if failures.Load() > 0 {
		return fmt.Errorf("%d/%d trigger calls failed", failures.Load(), calls.Load())
	}
	log.Printf("trigger calls: %d successful", calls.Load())
	return nil
}

func seconds(value float64) time.Duration {
	return time.Duration(value * float64(time.Second))
}
