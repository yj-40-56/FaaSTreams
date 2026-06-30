package com.ais;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSink extends RichSinkFunction<String> {
    private final String host;
    private final int port;
    private transient Jedis jedis;

    public RedisSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration config) {
        jedis = new Jedis(host, port);
    }

    @Override
    public void invoke(String value, Context context) {
        com.fasterxml.jackson.databind.ObjectMapper om =
                new com.fasterxml.jackson.databind.ObjectMapper();
        try {
            var node = om.readTree(value);
            double score = node.get("window_start").asDouble();
            jedis.zadd("analytics-results", score, value);
            System.out.printf("Window %s: vessels=%s records=%s latency=%sms%n",
                    node.get("window_start").asText(),
                    node.get("vessel_count").asText(),
                    node.get("records_processed").asText(),
                    node.get("latency_ms").asText()
            );
        } catch (Exception e) {
            System.err.println("RedisSink error: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
    }
}