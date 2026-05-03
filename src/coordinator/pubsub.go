package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

const (
	projectID      = "local-project"
	topicID        = "ais-stream"
	subscriptionID = "coordinator-sub"
)

// Pub/Sub returns the client, topic and subscription used to simulate local pub/sub broker
func setupPubSub(ctx context.Context) (*pubsub.Client, *pubsub.Topic, *pubsub.Subscription) {
	client, _ := pubsub.NewClient(ctx, projectID)

	topic := client.Topic(topicID)
	topicExists, _ := topic.Exists(ctx)
	if topicExists == false {
		topic, _ = client.CreateTopic(ctx, topicID)
	}

	subscription := client.Subscription(subscriptionID)
	subscriptionExists, _ := subscription.Exists(ctx)
	if subscriptionExists == false {
		subscription, _ = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
			Topic: topic,
		})
		fmt.Println("Subscription created")
	} else {
		fmt.Println("Subscription already exists")
	}

	return client, topic, subscription
}
