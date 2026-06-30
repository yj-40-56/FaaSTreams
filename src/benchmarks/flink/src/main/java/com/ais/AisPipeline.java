package com.ais;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class AisPipeline {

    static final String PROJECT    = "faastreams";
    static final String SUB        = "spe-input-sub";
    static final String REDIS_HOST = "10.101.64.19";
    static final int    REDIS_PORT = 6379;
    static final String QUERY      = "SELECT COUNT(DISTINCT mmsi) as vessel_count FROM vessels";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> rowStream = env
                .addSource(new PubSubSource(PROJECT, SUB))
                .filter(line -> line != null && !line.isEmpty())
                .map(line -> {
                    var node = new ObjectMapper().readTree(line);
                    return Row.of(
                            node.path("MMSI").asText(""),
                            node.path("Latitude").asDouble(0),
                            node.path("Longitude").asDouble(0),
                            node.path("SOG").asDouble(0),
                            node.path("# Timestamp").asText("")
                    );
                })
                .returns(Types.ROW_NAMED(
                        new String[]{"mmsi", "latitude", "longitude", "sog", "ts"},
                        Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.STRING
                ));

        tableEnv.createTemporaryView("vessels", rowStream,
                Schema.newBuilder()
                        .column("mmsi",      DataTypes.STRING())
                        .column("latitude",  DataTypes.DOUBLE())
                        .column("longitude", DataTypes.DOUBLE())
                        .column("sog",       DataTypes.DOUBLE())
                        .column("ts",        DataTypes.STRING())
                        .columnByExpression("proctime", "PROCTIME()")
                        .build()
        );

        Table resultTable = tableEnv.sqlQuery("""
                SELECT
                    TUMBLE_START(proctime, INTERVAL '30' SECOND) as window_start,
                    TUMBLE_END(proctime,   INTERVAL '30' SECOND) as window_end,
                    COUNT(DISTINCT mmsi) as vessel_count
                FROM vessels
                GROUP BY TUMBLE(proctime, INTERVAL '30' SECOND)
                """);

        // Table → DataStream → RedisSink
        tableEnv.toDataStream(resultTable)
                .map(row -> {
                    ObjectMapper om = new ObjectMapper();
                    ObjectNode result = om.createObjectNode();

                    LocalDateTime windowStart = (LocalDateTime) row.getField("window_start");
                    LocalDateTime windowEnd   = (LocalDateTime) row.getField("window_end");
                    long startEpoch = windowStart.toEpochSecond(ZoneOffset.UTC);
                    long endEpoch   = windowEnd.toEpochSecond(ZoneOffset.UTC);
                    long nowMs      = Instant.now().toEpochMilli();

                    result.put("pipeline",     "spe-flink");
                    result.put("query",        QUERY);
                    result.put("window_start", startEpoch);
                    result.put("window_end",   endEpoch);
                    result.put("vessel_count", (Long) row.getField("vessel_count"));
                    result.put("latency_ms",   nowMs - (endEpoch * 1000));
                    result.put("computed_at",  Instant.now().toString());

                    return om.writeValueAsString(result);
                })
                .returns(Types.STRING)
                .addSink(new RedisSink(REDIS_HOST, REDIS_PORT));

        env.execute("AIS SPE Pipeline");
    }
}