package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/faastreams/coordinator/source"

	"cloud.google.com/go/pubsub"
)

// Simulator reads events from a CSV file and published them to a Pub/sub topic, used for local testing
type Simulator struct {
	topic        *pubsub.Topic
	sourceConfig source.SourceConfig
}

func NewSimulator(topic *pubsub.Topic, sourceConfig source.SourceConfig) *Simulator {
	return &Simulator{
		topic:        topic,
		sourceConfig: sourceConfig,
	}
}

// Run Extract data from csv and publish each event to Pub/Sub topic
func (s *Simulator) Run(ctx context.Context) {
	file, err := os.Open(s.sourceConfig.CsvPath)
	if err != nil {
		log.Printf("[SIMULATOR] Failed to open CSV file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(s.sourceConfig.CsvDelimiter[0])
	reader.LazyQuotes = true
	csvHeaders, err := reader.Read()
	if err != nil {
		log.Printf("[Sim] Failed to read CSV header: %v\n", err)
		return
	}

	simulationStartReal := time.Now()
	var firstTimestampCSV time.Time
	var initialized bool

	// scaleFactor compresses CSV event timestamps so data plays back faster than it was recorded.
	// Formula: scaleFactor = CSV duration / desired real duration
	//
	// | CSV data | Real time | scaleFactor |
	// |----------|-----------|-------------|
	// | 24h      | 24h       | 1.0         |
	// | 24h      | 1h        | 24.0        |
	// | 24h      | 30min     | 48.0        |
	// | 24h      | 10min     | 144.0       |
	// | 1h       | 1min      | 60.0        |
	var publishedCount int

	log.Printf("[Sim] Starting simulation, scaleFactor=%.1f", s.sourceConfig.ScaleFactor)

	lineCount := 0
	for {
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

		currentTimeCSV, err := time.Parse(s.sourceConfig.TimestampFormat, record[s.sourceConfig.TimestampField])
		if err != nil {
			log.Printf("[SIMULATOR] Error: Could not parse timestamp '%s' in row: %v", record[s.sourceConfig.TimestampField], err)
			continue
		}

		if !initialized {
			firstTimestampCSV = currentTimeCSV
			initialized = true
		}

		elapsedTimeCSV := currentTimeCSV.Sub(firstTimestampCSV)
		scaledElapsedTime := time.Duration(float64(elapsedTimeCSV) / s.sourceConfig.ScaleFactor)
		newTimestamp := simulationStartReal.Add(scaledElapsedTime)

		waitTime := time.Until(newTimestamp)

		if waitTime > 5*time.Second {
			log.Printf("[Sim] Sleeping %s until next event at %s (CSV time %s)", waitTime.Round(time.Second), newTimestamp.Format(time.RFC3339), currentTimeCSV.Format(s.sourceConfig.TimestampFormat))
		}

		if lineCount%1000 == 0 {
			log.Printf("[Sim] Processing line %d, CSV-Time: %s", lineCount, record[s.sourceConfig.TimestampField])
		}

		if waitTime > 0 {
			time.Sleep(waitTime)
		}

		record[s.sourceConfig.TimestampField] = newTimestamp.Format(s.sourceConfig.TimestampFormat)

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
			log.Printf("[Sim] Published %d events so far, last CSV time %s", publishedCount, currentTimeCSV.Format(s.sourceConfig.TimestampFormat))
		}
	}
	s.topic.Flush()

	log.Printf("[Sim] Finished publishing all events, total published: %d", publishedCount)
}
