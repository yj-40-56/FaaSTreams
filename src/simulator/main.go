package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/faastreams/coordinator/source"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()

	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	topicID := os.Getenv("PUBSUB_TOPIC_ID")
	sourceConfigName := os.Getenv("SOURCE_CONFIG_NAME")
	sourceConfigPath := os.Getenv("SOURCE_CONFIG_PATH")

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

		// Batching: the client library accumulates calls to topic.Publish() and flushes
		// when any threshold is reached. This reduces API calls from one-per-event to
		// one-per-batch, enabling >20k events/s. Flush triggers on whichever fires first:
		// 100ms elapsed, 100 messages queued, or 1MB of payload.
		// FlowControl limits in-flight data to 1000 messages / 1GB; Block backpressures
		// the simulator instead of dropping or erroring when the limits are exceeded.
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

	config, err := source.LoadConfig(sourceConfigPath)
	if err != nil {
		log.Fatalf("Error while loading source config: %v", err)
	}

	sourceConfig := config.Sources[sourceConfigName]

	simulator := NewSimulator(topic, sourceConfig)
	simulator.Run(ctx)
}
