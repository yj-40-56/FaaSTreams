package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
)

// Playback is the whole simulator config, read from env vars only. The simulator
// isn't deployed, so it doesn't touch the shared query config even where that
// means duplicating a value (TimestampField, TimestampFormat).
//
// ScaleFactor compresses CSV event timestamps so data plays back faster than it was recorded.
// Formula: scaleFactor = CSV duration / desired real duration
//
// | CSV data | Real time | scaleFactor |
// |----------|-----------|-------------|
// | 24h      | 24h       | 1.0         |
// | 24h      | 1h        | 24.0        |
// | 24h      | 30min     | 48.0        |
// | 24h      | 10min     | 144.0       |
// | 1h       | 1min      | 60.0        |
type Playback struct {
	CsvPath      string
	CsvDelimiter string
	ScaleFactor  float64

	// The CSV column holding the event time, and its Go layout. Must match
	// timestamp_field/timestamp_format in the query config, or the ingestor
	// drops every event.
	TimestampField  string
	TimestampFormat string

	// Stamps the source line number, so identical rows cannot collapse into
	// one ZADD member. Empty disables it.
	SeqField string
}

// loadPlayback reads the config from env vars. CSV path and timestamp settings
// are required, the rest defaults.
func loadPlayback() Playback {
	playback := Playback{
		CsvPath:         os.Getenv("SIM_CSV_PATH"),
		CsvDelimiter:    ",",
		ScaleFactor:     1.0,
		TimestampField:  os.Getenv("SIM_TIMESTAMP_FIELD"),
		TimestampFormat: os.Getenv("SIM_TIMESTAMP_FORMAT"),
		SeqField:        os.Getenv("SIM_SEQ_FIELD"),
	}

	if playback.CsvPath == "" {
		log.Fatal("[Sim] SIM_CSV_PATH env var required")
	}
	if playback.TimestampField == "" {
		log.Fatal("[Sim] SIM_TIMESTAMP_FIELD env var required")
	}
	if playback.TimestampFormat == "" {
		log.Fatal("[Sim] SIM_TIMESTAMP_FORMAT env var required")
	}

	if raw := os.Getenv("SIM_CSV_DELIMITER"); raw != "" {
		playback.CsvDelimiter = raw
	}

	if raw := os.Getenv("SIM_SCALE_FACTOR"); raw != "" {
		scaleFactor, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			log.Fatalf("[Sim] Invalid SIM_SCALE_FACTOR %q: %v", raw, err)
		}
		if scaleFactor <= 0 {
			log.Fatalf("[Sim] SIM_SCALE_FACTOR must be positive, got %.1f", scaleFactor)
		}
		playback.ScaleFactor = scaleFactor
	}

	return playback
}

// Simulator reads events from a CSV file and published them to a Pub/sub topic, used for local testing
type Simulator struct {
	topic      *pubsub.Topic
	sourceName string
	playback   Playback
	runtime    time.Duration
}

func NewSimulator(topic *pubsub.Topic, sourceName string, playback Playback, runtime time.Duration) *Simulator {
	return &Simulator{
		topic:      topic,
		sourceName: sourceName,
		playback:   playback,
		runtime:    runtime,
	}
}

// Run Extract data from csv and publish each event to Pub/Sub topic
func (s *Simulator) Run(ctx context.Context) {
	file, err := os.Open(s.playback.CsvPath)
	if err != nil {
		log.Printf("[SIMULATOR] Failed to open CSV file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if s.playback.CsvDelimiter != "" {
		reader.Comma = rune(s.playback.CsvDelimiter[0])
	}
	reader.LazyQuotes = true
	csvHeaders, err := reader.Read()
	if err != nil {
		log.Printf("[Sim] Failed to read CSV header: %v\n", err)
		return
	}

	simulationStartReal := time.Now()
	var firstTimestampCSV, lastTimestampCSV time.Time
	var initialized bool
	var publishedCount int

	var deadline time.Time
	if s.runtime > 0 {
		deadline = simulationStartReal.Add(s.runtime)
		log.Printf("[Sim] Starting simulation, scaleFactor=%.1f, runtime=%s", s.playback.ScaleFactor, s.runtime)
	} else {
		log.Printf("[Sim] Starting simulation, scaleFactor=%.1f", s.playback.ScaleFactor)
	}

	lineCount := 0
	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			log.Printf("[Sim] Stopping: runtime budget (%s) reached. Published %d lines, simulated time covered %s -> %s (span %s)",
				s.runtime, publishedCount, firstTimestampCSV.Format(s.playback.TimestampFormat), lastTimestampCSV.Format(s.playback.TimestampFormat), lastTimestampCSV.Sub(firstTimestampCSV))
			break
		}

		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
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

		currentTimeCSV, err := time.Parse(s.playback.TimestampFormat, record[s.playback.TimestampField])
		if err != nil {
			log.Printf("[SIMULATOR] Error: Could not parse timestamp '%s' in row: %v", record[s.playback.TimestampField], err)
			continue
		}

		if !initialized {
			firstTimestampCSV = currentTimeCSV
			initialized = true
		}
		lastTimestampCSV = currentTimeCSV

		elapsedTimeCSV := currentTimeCSV.Sub(firstTimestampCSV)
		scaledElapsedTime := time.Duration(float64(elapsedTimeCSV) / s.playback.ScaleFactor)
		newTimestamp := simulationStartReal.Add(scaledElapsedTime)

		waitTime := time.Until(newTimestamp)

		if waitTime > 5*time.Second {
			log.Printf("[Sim] Sleeping %s until next event at %s (CSV time %s)", waitTime.Round(time.Second), newTimestamp.Format(time.RFC3339), currentTimeCSV.Format(s.playback.TimestampFormat))
		}

		if lineCount%1000 == 0 {
			log.Printf("[Sim] Processing line %d, CSV-Time: %s", lineCount, record[s.playback.TimestampField])
		}
		if waitTime > 0 {
			time.Sleep(waitTime)
		}

		record[s.playback.TimestampField] = newTimestamp.UTC().Format(s.playback.TimestampFormat)

		record["_source"] = s.sourceName
		if s.playback.SeqField != "" {
			record[s.playback.SeqField] = strconv.Itoa(lineCount)
		}
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
			log.Printf("[Sim] Published %d events so far, last CSV time %s", publishedCount, currentTimeCSV.Format(s.playback.TimestampFormat))
		}
	}
	s.topic.Flush()

	log.Printf("[Sim] Finished publishing all events, total published: %d", publishedCount)
}
