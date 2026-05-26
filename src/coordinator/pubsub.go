package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
)

func setupPubSub(ctx context.Context) (*pubsub.Client, *pubsub.Topic, *pubsub.Subscription) {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	topicID := os.Getenv("PUBSUB_TOPIC_ID")
	subscriptionID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("[PubSub] Failed to create Pub/Sub client: %v", err)
	}

	topic := client.Topic(topicID)
	subscription := client.Subscription(subscriptionID)

	mode := os.Getenv("RUN_MODE")
	if mode == "http" {
		log.Println("[PubSub] HTTP mode - using existing topic and subscription")
		return client, topic, subscription
	}

	// Local mode - create topic and subscription if not exists
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatalf("[PubSub] Failed to check if topic exists: %v", err)
	}
	if topicExists == false {
		topic, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			log.Fatalf("[PubSub] Failed to create topic: %v", err)
		}
	} else {
		log.Printf("[PubSub] Topic already exists: %s", topicID)
	}

	subscriptionExists, err := subscription.Exists(ctx)
	if err != nil {
		log.Fatalf("[PubSub] Failed to check if subscription exists: %v", err)
	}
	if subscriptionExists == false {
		subscription, err = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
			Topic: topic,
		})
		if err != nil {
			log.Fatalf("[PubSub] Failed to create subscription: %v", err)
		}
	} else {
		log.Println("[PubSub] Subscription already exists")
	}

	return client, topic, subscription
}
