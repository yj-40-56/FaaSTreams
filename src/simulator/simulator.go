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
	topic   *pubsub.Topic
	csvPath string
}

func NewSimulator(topic *pubsub.Topic, csvPath string) *Simulator {
	return &Simulator{
		topic:   topic,
		csvPath: csvPath,
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
	const scaleFactor = 24.0

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		record := make(map[string]string)
		for i := 0; i < len(csvHeaders); i++ {
			record[csvHeaders[i]] = row[i]
		}

		currentTimeCSV, err := time.Parse("02/01/2006 15:04:05", record["# Timestamp"])
		if err != nil {
			log.Printf("[SIMULATOR] Error: Could not parse timestamp '%s' in row: %v", record["# Timestamp"], err)
			continue
		}

		if !initialized {
			firstTimestampCSV = currentTimeCSV
			initialized = true
		}

		elapsedTimeCSV := currentTimeCSV.Sub(firstTimestampCSV)
		scaledElapsedTime := time.Duration(float64(elapsedTimeCSV) / scaleFactor)
		newTimestamp := simulationStartReal.Add(scaledElapsedTime)

		waitTime := time.Until(newTimestamp)

		if waitTime > 0 {
			time.Sleep(waitTime)
		}

		record["# Timestamp"] = newTimestamp.Format("02/01/2006 15:04:05")

		messageBytes, err := json.Marshal(record)
		if err != nil {
			continue
		}
		s.topic.Publish(ctx, &pubsub.Message{Data: messageBytes})
	}
	s.topic.Flush()

	log.Printf("[Sim] Finished publishing all events")
}
