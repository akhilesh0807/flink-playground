package com.example.flink.jobs;

import com.example.flink.dto.MovingAvg;
import com.example.flink.dto.TickOutRecord;
import com.example.flink.dto.TickRecord;
import com.example.flink.functions.MovingAvgAggregatorUsingWindow;
import com.example.flink.functions.MovingAvgCalculator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;


public class TickIngestJob {

    public static void main(String[] args) throws Exception {
        // 1. Set up the local Flink streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure checkpointing for fault-tolerance testing (optional but recommended)
        env.enableCheckpointing(5000); // Checkpoint every 5 seconds

        KafkaSource<TickRecord> source = KafkaSource.<TickRecord>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("tick")
            .setGroupId("default-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new JsonDeserializationSchema<>(TickRecord.class))
            .build();

        WatermarkStrategy<TickRecord> monotonousWatermarkStrategy = WatermarkStrategy
            .<TickRecord>forMonotonousTimestamps()
                .withTimestampAssigner((r,s) -> r.timestamp()*1000);

        WatermarkStrategy<TickRecord> outOrderWatermarkStrategy = WatermarkStrategy
            .<TickRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
            .withTimestampAssigner((r,s) -> r.timestamp()*1000);

//        KafkaSink<TickOutRecord> sink = KafkaSink.<TickOutRecord>builder()
//            .setBootstrapServers("kafka:29092")
//            .setRecordSerializer(
//                KafkaRecordSerializationSchema.<TickOutRecord>builder()
//                    .setTopic("tick-out")
//                    .setValueSerializationSchema(new JsonSerializationSchema<>())
//                    .build())
//            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//            .build();

        KafkaSink<TickOutRecord> sink = KafkaSink.<TickOutRecord>builder()
            .setBootstrapServers("kafka:29092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.<TickOutRecord>builder()
                    .setTopic("tick-out")
                    .setValueSerializationSchema(new JsonSerializationSchema<>())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

//        env.fromSource(source, monotonousWatermarkStrategy, "Kafka Source")
//            .keyBy(r -> "GLOBAL")
//            .process(new MovingAvgCalculator(5))
//            .sinkTo(sink);

        env.fromSource(source, outOrderWatermarkStrategy, "Kafka Source")
            .keyBy(r -> "GLOBAL")
            .window(SlidingEventTimeWindows.of(Duration.ofDays(5), Duration.ofDays(1)))
            .aggregate(new MovingAvgAggregatorUsingWindow())
            .sinkTo(sink);

        env.execute();

    }
}
