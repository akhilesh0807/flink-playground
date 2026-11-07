package com.example.flink.source.zmq.utils;
import com.example.flink.source.zmq.core.ZeroMQSourceSplit;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.util.Collector;

public class ZeroMQRecordEmitter<OUT> implements RecordEmitter<byte[], OUT, ZeroMQSourceSplit> {
    private final DeserializationSchema<OUT> deserializer;
    private final SimpleCollector<OUT> collector = new SimpleCollector<>();

    public ZeroMQRecordEmitter(DeserializationSchema<OUT> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void emitRecord(
        byte[] element,
        SourceOutput<OUT> output,
        ZeroMQSourceSplit split) throws Exception {

        // Deserialize the raw ZMQ byte frame
        collector.setOutput(output);
        deserializer.deserialize(element, collector);
    }

    // A lightweight internal collector targeting Flink's internal source output pipeline
    private static final class SimpleCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        void setOutput(SourceOutput<T> output) {
            this.output = output;
        }

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {}
    }
}
