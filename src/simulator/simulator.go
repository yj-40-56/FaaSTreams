package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"simulator/config"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

// Simulator reads events from a CSV file (local path or gs:// path) and publishes them to a Pub/Sub topic
type Simulator struct {
	topic      *pubsub.Topic
	sourceName string
	source     config.Source
	runtime    time.Duration
}

func NewSimulator(topic *pubsub.Topic, sourceName string, source config.Source, runtime time.Duration) *Simulator {
	return &Simulator{
		topic:      topic,
		sourceName: sourceName,
		source:     source,
		runtime:    runtime,
	}
}

// openCsvSource opens the CSV either from GCS (if CsvPath starts with gs://) or from local disk.
// Returns a ReadCloser so the caller can defer Close() the same way regardless of source.
func openCsvSource(ctx context.Context, path string) (io.ReadCloser, error) {
	if strings.HasPrefix(path, "gs://") {
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
		// gs://bucket/object/path.csv -> split into bucket + object
		trimmed := strings.TrimPrefix(path, "gs://")
		parts := strings.SplitN(trimmed, "/", 2)
		bucket, object := parts[0], parts[1]

		reader, err := client.Bucket(bucket).Object(object).NewReader(ctx)
		if err != nil {
			return nil, err
		}
		return reader, nil // storage.Reader closes the underlying client connection on Close()
	}

	// Local file fallback (still supported for local testing)
	return os.Open(path)
}

// Run extracts data from the CSV source and publishes each event to Pub/Sub
func (s *Simulator) Run(ctx context.Context) {
	file, err := openCsvSource(ctx, s.source.CsvPath)
	if err != nil {
		log.Printf("[SIMULATOR] Failed to open CSV source %q: %v\n", s.source.CsvPath, err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if s.source.CsvDelimiter != "" {
		reader.Comma = rune(s.source.CsvDelimiter[0])
	}
	reader.LazyQuotes = true
	csvHeaders, err := reader.Read()
	if err != nil {
		log.Printf("[Sim] Failed to read CSV header: %v\n", err)
		return
	}

	// Blackout window: drop all events whose original CSV timestamp falls into this window,
	// simulating a real gap in the data.
	// Just set these two values directly for your run (CSV hour X to Y where the gap should be):
	blackoutStart := 8 * time.Hour
	blackoutEnd := 16 * time.Hour
	var blackoutApplied bool

	simulationStartReal := time.Now()
	var firstTimestampCSV, lastTimestampCSV time.Time
	var initialized bool
	var publishedCount int

	var deadline time.Time
	if s.runtime > 0 {
		deadline = simulationStartReal.Add(s.runtime)
		log.Printf("[Sim] Starting simulation, scaleFactor=%.1f, runtime=%s", s.source.ScaleFactor, s.runtime)
	} else {
		log.Printf("[Sim] Starting simulation, scaleFactor=%.1f", s.source.ScaleFactor)
	}

	lineCount := 0
	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			log.Printf("[Sim] Stopping: runtime budget (%s) reached. Published %d lines, simulated time covered %s -> %s (span %s)",
				s.runtime, publishedCount, firstTimestampCSV.Format(s.source.TimestampFormat), lastTimestampCSV.Format(s.source.TimestampFormat), lastTimestampCSV.Sub(firstTimestampCSV))
			break
		}

		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				log.Printf("[Sim] Finished: Reached end of file. Total lines: %d", lineCount)
			} else {
				log.Printf("[Sim] ERROR: Reader stopped at line %d: %v", lineCount, err)
			}
			break
		}
		lineCount++

		record := make(map[string]string)
		for i := 0; i < len(csvHeaders); i++ {
			record[csvHeaders[i]] = row[i]
		}

		currentTimeCSV, err := time.Parse(s.source.TimestampFormat, record[s.source.TimestampField])
		if err != nil {
			log.Printf("[SIMULATOR] Error: Could not parse timestamp '%s' in row: %v", record[s.source.TimestampField], err)
			continue
		}

		if !initialized {
			firstTimestampCSV = currentTimeCSV
			initialized = true
		}
		lastTimestampCSV = currentTimeCSV

		elapsedTimeCSV := currentTimeCSV.Sub(firstTimestampCSV)

		// If this event falls inside the blackout window, drop it.
		// The first time we enter the window, sleep for the full (scaled) blackout duration once,
		// then keep dropping rows still inside it, then resume normally afterwards.
		if elapsedTimeCSV >= blackoutStart && elapsedTimeCSV < blackoutEnd {
			if !blackoutApplied {
				blackoutDuration := blackoutEnd - blackoutStart
				scaledBlackoutDuration := time.Duration(float64(blackoutDuration) / s.source.ScaleFactor)
				log.Printf("[Sim] Entering blackout window: sleeping %s real time (CSV %s -> %s)",
					scaledBlackoutDuration, blackoutStart, blackoutEnd)
				time.Sleep(scaledBlackoutDuration)
				simulationStartReal = simulationStartReal.Add(scaledBlackoutDuration)
				blackoutApplied = true
			}
			continue // drop this row, don't publish
		}

		scaledElapsedTime := time.Duration(float64(elapsedTimeCSV) / s.source.ScaleFactor)
		newTimestamp := simulationStartReal.Add(scaledElapsedTime)

		waitTime := time.Until(newTimestamp)

		if waitTime > 5*time.Second {
			log.Printf("[Sim] Sleeping %s until next event at %s (CSV time %s)", waitTime.Round(time.Second), newTimestamp.Format(time.RFC3339), currentTimeCSV.Format(s.source.TimestampFormat))
		}

		if lineCount%1000 == 0 {
			log.Printf("[Sim] Processing line %d, CSV-Time: %s", lineCount, record[s.source.TimestampField])
		}
		if waitTime > 0 {
			time.Sleep(waitTime)
		}

		record[s.source.TimestampField] = newTimestamp.UTC().Format(s.source.TimestampFormat)

		record["_source"] = s.sourceName
		messageBytes, err := json.Marshal(record)
		if err != nil {
			log.Printf("[SIMULATOR] JSON error at line %d: %v", lineCount, err)
			continue
		}

		result := s.topic.Publish(ctx, &pubsub.Message{Data: messageBytes})
		go func() {
			if _, err := result.Get(ctx); err != nil {
				log.Printf("[Sim] Failed to publish message: %v", err)
			}
		}()

		publishedCount++
		if publishedCount%50 == 0 {
			log.Printf("[Sim] Published %d events so far, last CSV time %s", publishedCount, currentTimeCSV.Format(s.source.TimestampFormat))
		}
	}
	s.topic.Flush()

	log.Printf("[Sim] Finished publishing all events, total published: %d", publishedCount)
}
