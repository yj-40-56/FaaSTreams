package com.ais;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

public class PubSubSource extends RichSourceFunction<String> {
    private final String project;
    private final String subscription;
    private volatile boolean running = true;

    public PubSubSource(String project, String subscription) {
        this.project      = project;
        this.subscription = subscription;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        SubscriberStubSettings settings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(
                        SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                .setMaxInboundMessageSize(20 * 1024 * 1024)
                                .build()
                )
                .build();

        try (GrpcSubscriberStub subscriber = GrpcSubscriberStub.create(settings)) {
            String subName = ProjectSubscriptionName.format(project, subscription);
            PullRequest pullRequest = PullRequest.newBuilder()
                    .setMaxMessages(500)
                    .setSubscription(subName)
                    .build();

            while (running) {
                PullResponse response = subscriber.pullCallable().call(pullRequest);
                List<ReceivedMessage> messages = response.getReceivedMessagesList();

                if (messages.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                for (ReceivedMessage msg : messages) {
                    ctx.collect(msg.getMessage().getData().toStringUtf8());
                }

                // Ack
                List<String> ackIds = messages.stream()
                        .map(ReceivedMessage::getAckId)
                        .toList();
                AcknowledgeRequest ackRequest = AcknowledgeRequest.newBuilder()
                        .setSubscription(subName)
                        .addAllAckIds(ackIds)
                        .build();
                subscriber.acknowledgeCallable().call(ackRequest);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}