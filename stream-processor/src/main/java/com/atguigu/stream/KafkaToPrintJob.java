package com.atguigu.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

public class KafkaToPrintJob {

    public static void main(String[] args) throws Exception {

        // ---- env / config ----
        String bootstrap = getenvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        String inputTopic = getenvOrDefault("INPUT_TOPIC", "user-events-raw-v2");
        String cleanTopic = getenvOrDefault("CLEAN_TOPIC", "clean-events");
        String dlqTopic = getenvOrDefault("DLQ_TOPIC", "dlq-events");
        String groupId = getenvOrDefault("KAFKA_GROUP_ID", "stream-processor-v1");

        // ---- flink env ----
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ---- kafka source (raw json string) ----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                // 为了你测试方便：从最早开始读（有历史消息也能读到）
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-user-events");

        // ---- add ingestTime prefix ----
        DataStream<String> pretty = raw.map(v ->
                        "ingestTime=" + Instant.now().toString() + " | " + v
                )
                .name("prefix-with-time");

        // ---- route clean vs dlq ----
        final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq") {};

        SingleOutputStreamOperator<String> cleanStream =
                pretty.process(new ValidateAndRouteProcessFunction(DLQ_TAG))
                        .name("validate-and-route");

        DataStream<String> dlqStream = cleanStream.getSideOutput(DLQ_TAG);

        // ---- kafka sinks ----
        KafkaSink<String> cleanSink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(cleanTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build();

        KafkaSink<String> dlqSink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(dlqTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build();

        cleanStream.sinkTo(cleanSink).name("sink-clean-events");
        dlqStream.sinkTo(dlqSink).name("sink-dlq-events");

        env.execute("Kafka -> Validate -> (Clean/DLQ) -> Kafka");
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? defaultValue : v;
    }
}