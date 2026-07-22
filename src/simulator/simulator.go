package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"time"

	"simulator/config"

	"cloud.google.com/go/pubsub"
)

// Simulator reads events from a CSV file and published them to a Pub/sub topic, used for local testing
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

// Run Extract data from csv and publish each event to Pub/Sub topic
func (s *Simulator) Run(ctx context.Context) {
	file, err := os.Open(s.source.CsvPath)
	if err != nil {
		log.Printf("[SIMULATOR] Failed to open CSV file: %v\n", err)
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

		// If this event falls inside the blackout window (second hour), drop it.
		// The first time we enter the window, sleep for the full blackout duration once,
		// then keep dropping rows still inside it, then resume normally afterwards.
		if elapsedTimeCSV >= blackoutStart && elapsedTimeCSV < blackoutEnd {
			if !blackoutApplied {
				blackoutDuration := blackoutEnd - blackoutStart
				scaledBlackoutDuration := time.Duration(float64(blackoutDuration) / s.source.ScaleFactor)
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
