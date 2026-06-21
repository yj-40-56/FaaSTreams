package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

// Simulator reads events from a CSV file and published them to a Pub/sub topic, used for local testing
type Simulator struct {
	topic      *pubsub.Topic
	csvPath    string
	sourceName string
}

func NewSimulator(topic *pubsub.Topic, csvPath string) *Simulator {
	return &Simulator{
		topic:      topic,
		csvPath:    csvPath,
		sourceName: os.Getenv("SOURCE_NAME"),
	}
}

// Run Extract data from csv and publish each event to Pub/Sub topic
func (s *Simulator) Run(ctx context.Context) {
	file, err := os.Open(s.csvPath)
	if err != nil {
		log.Printf("[SIMULATOR] Failed to open CSV file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','
	reader.LazyQuotes = true
	csvHeaders, err := reader.Read()
	if err != nil {
		log.Printf("[Sim] Failed to read CSV header: %v\n", err)
		return
	}

	simulationStartReal := time.Now()
	var firstTimestampCSV time.Time
	var initialized bool
	const scaleFactor = 60.0

	// Append each row into slice
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

		timestampStr := "Timestamp"
		if s.sourceName == "ais_data_v1" {
			timestampStr = "# Timestamp"
		}
		currentTimeCSV, err := time.Parse("02/01/2006 15:04:05", record[timestampStr])
		if err != nil {
			log.Printf("[SIMULATOR] Error: Could not parse timestamp '%s' in row: %v", record[timestampStr], err)
			continue
		}

		if !initialized {
			firstTimestampCSV = currentTimeCSV
			initialized = true
		}

		elapsedTimeCSV := currentTimeCSV.Sub(firstTimestampCSV)
		scaledElapsedTime := time.Duration(float64(elapsedTimeCSV) / scaleFactor)
		newTimestamp := simulationStartReal.Add(scaledElapsedTime)

		if lineCount%1000 == 0 {
			log.Printf("DEBUG: Processing line %d, CSV-Time: %s", lineCount, record[timestampStr])
		}

		record[timestampStr] = newTimestamp.Format("02/01/2006 15:04:05")

		time.Sleep(time.Until(newTimestamp))

		record["_source"] = s.sourceName
		messageBytes, err := json.Marshal(record)
		if err != nil {
			log.Printf("[SIMULATOR] JSON error at line %d: %v", lineCount, err)
			continue
		}
		s.topic.Publish(ctx, &pubsub.Message{Data: messageBytes})

	}
	s.topic.Flush()

	log.Printf("[Sim] Finished publishing all events")
}
