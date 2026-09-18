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

	// Published on every event as "_source", the ingestor routes on it. Has to
	// match a key under sources: in the query config
	sourceName := os.Getenv("SOURCE_NAME")
	if sourceName == "" {
		log.Fatal("[Sim] SOURCE_NAME env var required")
	}

	var runtime time.Duration
	if raw := os.Getenv("SIM_RUNTIME"); raw != "" {
		var err error
		runtime, err = time.ParseDuration(raw)
		if err != nil {
			log.Fatalf("[Sim] Invalid SIM_RUNTIME %q: %v", raw, err)
		}
	}

	playback := loadPlayback()

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

		// Batching cuts API calls from one per event to one per batch, enabling
		// >20k events/s. Flushes on whichever comes first: 100ms, 100 messages,
		// 1MB. FlowControl caps in-flight at 1000 messages / 1GB, and Block
		// backpressures rather than dropping or erroring.
		topic.PublishSettings.DelayThreshold = 100 * time.Millisecond
		topic.PublishSettings.CountThreshold = 100
		topic.PublishSettings.ByteThreshold = 1e6
		topic.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
			MaxOutstandingMessages: 1000,
			MaxOutstandingBytes:    1e9,
			LimitExceededBehavior:  pubsub.FlowControlBlock,
		}

		if exists {
			log.Println("[Sim] Topic found")
			break
		}
		log.Println("[Sim] Topic not found yet retrying in 2s...")
		time.Sleep(2 * time.Second)
	}

	time.Sleep(5 * time.Second)
	simulator := NewSimulator(topic, sourceName, playback, runtime)
	simulator.Run(ctx)
}
