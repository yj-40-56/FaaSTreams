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
import org.apache.sedona.flink.SedonaFlinkRegistrator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class AisPipeline {

    static final String PROJECT    = "faastreams";
    static final String SUB        = "spe-input-sub";
    static final String REDIS_HOST = "10.101.64.19";
    static final int    REDIS_PORT = 6379;

    static final String QUERY = """
            SELECT
              v.mmsi,
              v.sog,
              v.ts,
              t.tower_name,
              t.threshold_nm,
              ROUND(
                ST_Distance(
                  ST_Transform(ST_Point(v.longitude, v.latitude), 'EPSG:4326', 'EPSG:3857'),
                  ST_Transform(ST_GeomFromText(t.geom_wkt),       'EPSG:4326', 'EPSG:3857')
                ) / 1852.0, 2
              ) AS distance_nm
            FROM vessels v
            CROSS JOIN towers t
            WHERE v.latitude IS NOT NULL
              AND ST_Distance(
                    ST_Transform(ST_Point(v.longitude, v.latitude), 'EPSG:4326', 'EPSG:3857'),
                    ST_Transform(ST_GeomFromText(t.geom_wkt),       'EPSG:4326', 'EPSG:3857')
                  ) / 1852.0 < t.threshold_nm
            """;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);

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

        tableEnv.createTemporaryView("towers", buildTowersTable(tableEnv));

        Table resultTable = tableEnv.sqlQuery(QUERY);

        tableEnv.toDataStream(resultTable)
                .map(row -> {
                    ObjectMapper om = new ObjectMapper();
                    ObjectNode result = om.createObjectNode();

                    String mmsi       = (String) row.getField("mmsi");
                    Double sog        = (Double) row.getField("sog");
                    String ts         = (String) row.getField("ts");
                    String towerName  = (String) row.getField("tower_name");
                    Double thresholdNm = (Double) row.getField("threshold_nm");
                    Double distanceNm = (Double) row.getField("distance_nm");

                    String alertMsg = String.format(
                            "VESSEL %s passed within %s nm of %s | sog=%s kn | ts=%s",
                            mmsi, distanceNm, towerName, sog, ts
                    );

                    result.put("pipeline",      "spe-flink");
                    result.put("query",         QUERY);
                    result.put("mmsi",          mmsi);
                    result.put("sog",           sog);
                    result.put("ts",            ts);
                    result.put("tower_name",    towerName);
                    result.put("threshold_nm",  thresholdNm);
                    result.put("distance_nm",   distanceNm);
                    result.put("is_alert",      true);
                    result.put("alert_message", alertMsg);
                    result.put("computed_at",   Instant.now().toString());

                    return om.writeValueAsString(result);
                })
                .returns(Types.STRING)
                .addSink(new RedisSink(REDIS_HOST, REDIS_PORT));

        env.execute("AIS SPE Pipeline");
    }

    private static Table buildTowersTable(StreamTableEnvironment tableEnv) {
        List<Row> towerRows = new ArrayList<>();
        towerRows.add(Row.of("Tower Alpha", "POINT(10.2 57.1)", 5.0));
        towerRows.add(Row.of("Tower Beta",  "POINT(7.8 55.7)",  5.0));
        towerRows.add(Row.of("Tower Gamma", "POINT(10.7 58.2)", 5.0));

        return tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("tower_name",   DataTypes.STRING()),
                        DataTypes.FIELD("geom_wkt",      DataTypes.STRING()),
                        DataTypes.FIELD("threshold_nm",  DataTypes.DOUBLE())
                ),
                towerRows
        );
    }
}