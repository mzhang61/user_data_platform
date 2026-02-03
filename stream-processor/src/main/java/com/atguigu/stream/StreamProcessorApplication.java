package com.atguigu.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

public class StreamProcessorApplication {

    // Topics (can be overridden by environment variables)
    private static final String TOPIC_IN = env("KAFKA_TOPIC_IN", "user-events-raw-v2");
    private static final String TOPIC_CLEAN = env("KAFKA_TOPIC_CLEAN", "clean-events");
    private static final String TOPIC_DLQ = env("KAFKA_TOPIC_DLQ", "dlq-events");

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        String groupId = env("KAFKA_GROUP_ID", "stream-processor-v1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Print the REAL runtime config so you never get confused by operator names again
        System.out.println("[stream-processor] bootstrap=" + bootstrap
                + " topicIn=" + TOPIC_IN
                + " cleanTopic=" + TOPIC_CLEAN
                + " dlqTopic=" + TOPIC_DLQ
                + " groupId=" + groupId);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(TOPIC_IN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // IMPORTANT: the 3rd arg is ONLY an operator name (NOT the Kafka topic)
        // Use TOPIC_IN directly to avoid misleading strings like "kafka-" + TOPIC_IN
        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                TOPIC_IN
        );

        // Force a consistent line format:
        // ingestTime=... | {json}
        DataStream<String> withIngestTime = raw
                .map((MapFunction<String, String>) line ->
                        "ingestTime=" + Instant.now() + " | " + line
                )
                .name("prefix-with-time");

        DataStream<RedisWriters.RouteResult> routed = withIngestTime
                .map((MapFunction<String, RedisWriters.RouteResult>) RedisWriters::validateAndRoute)
                .name("validate-and-route");

        DataStream<String> clean = routed
                .filter(r -> r.kind() == RedisWriters.Kind.CLEAN)
                .map((MapFunction<RedisWriters.RouteResult, String>) RedisWriters.RouteResult::payload)
                .name("clean-stream");

        DataStream<String> dlq = routed
                .filter(r -> r.kind() == RedisWriters.Kind.DLQ)
                .map((MapFunction<RedisWriters.RouteResult, String>) RedisWriters.RouteResult::payload)
                .name("dlq-stream");

        // ---- Kafka sinks ----
        KafkaSink<String> cleanKafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(TOPIC_CLEAN)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<String> dlqKafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(TOPIC_DLQ)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        clean.sinkTo(cleanKafkaSink).name("sink-clean-events");
        dlq.sinkTo(dlqKafkaSink).name("sink-dlq-events");

        // ---- Redis sinks ----
        clean.addSink(new RedisWriters.CleanRedisSink()).name("redis-clean-writer");
        dlq.addSink(new RedisWriters.DlqRedisSink()).name("redis-dlq-writer");

        // Print execution plan so you can confirm Redis sinks exist in the job
        System.out.println(env.getExecutionPlan());

        env.execute("stream-processor");
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v.trim();
    }
}