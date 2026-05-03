package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

// Simulator reads events from a CSV file and published them
// to a Pub/sub topic
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
		fmt.Printf("Failed to open CSV file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Extract header with column names
	headers, err := reader.Read()
	if err != nil {
		fmt.Printf("Failed to read CSV header: %v\n", err)
		return
	}

	// Create slice of all rows, where row has columns as key and values as value
	var allRows []map[string]string

	// Append each row into slice
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		record := make(map[string]string)
		for i := 0; i < len(headers); i++ {
			record[headers[i]] = row[i]
		}

		allRows = append(allRows, record)
	}

	fmt.Printf("Read %d rows from CSV\n", len(allRows))

	// Publish each event to Pub/Sub
	for i := 0; i < len(allRows); i++ {
		messageBytes, err := json.Marshal(allRows[i])
		if err != nil {
			continue
		}

		s.topic.Publish(ctx, &pubsub.Message{
			Data: messageBytes,
		})
		time.Sleep(1 * time.Millisecond)
	}

	fmt.Println("Finished publishing all events")
}
