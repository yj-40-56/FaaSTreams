package main

import (
	"context"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()

	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	topicID := os.Getenv("PUBSUB_TOPIC_ID")
	csvPath := os.Getenv("CSV_PATH")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("[Sim] Failed to create client: %v", err)
	}
	defer client.Close()

	// Wait for topic to exist
	log.Println("[Sim] Waiting for topic...")
	var topic *pubsub.Topic
	for {
		topic = client.Topic(topicID)
		exists, err := topic.Exists(ctx)
		if err != nil {
			log.Printf("[Sim] Error checking topic: %v retrying...\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if exists {
			log.Println("[Sim] Topic found")
			break
		}
		log.Println("[Sim] Topic not found yet retrying in 2s...")
		time.Sleep(2 * time.Second)
	}

	time.Sleep(5 * time.Second)
	simulator := NewSimulator(topic, csvPath)
	simulator.Run(ctx)
}
