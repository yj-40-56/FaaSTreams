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
//            var node = om.readTree(value);

            double score = System.currentTimeMillis();

            jedis.zadd("flink-results", score, value);

//            System.out.printf("ALERT mmsi=%s tower=%s distance_nm=%s sog=%s ts=%s%n",
//                    node.path("mmsi").asText("?"),
//                    node.path("tower_name").asText("?"),
//                    node.path("distance_nm").asText("?"),
//                    node.path("sog").asText("?"),
//                    node.path("ts").asText("?")
//            );
        } catch (Exception e) {
            System.err.println("RedisSink error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
    }
}