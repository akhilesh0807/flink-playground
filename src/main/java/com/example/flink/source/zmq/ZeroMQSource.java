package com.example.flink.source.zmq;

import com.example.flink.source.zmq.core.ZeroMQSourceSplit;
import com.example.flink.source.zmq.core.ZeroMQSplitEnumerator;
import com.example.flink.source.zmq.core.ZeroMQSplitReader;
import com.example.flink.source.zmq.utils.ZeroMQEnumStateSerializer;
import com.example.flink.source.zmq.utils.ZeroMQRecordEmitter;
import com.example.flink.source.zmq.utils.ZeroMQSplitSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ZeroMQSource<OUT> implements Source<OUT, ZeroMQSourceSplit, Collection<ZeroMQSourceSplit>>,  ResultTypeQueryable<OUT> {
    private final List<ZeroMQSourceSplit> targetConnections;
    private final DeserializationSchema<OUT> deserializer;

    public ZeroMQSource(List<ZeroMQSourceSplit> targetConnections, DeserializationSchema<OUT> deserializer) {
        this.targetConnections = targetConnections;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, ZeroMQSourceSplit> createReader(SourceReaderContext readerContext) {
        return new SingleThreadMultiplexSourceReaderBase<>(
            () -> new ZeroMQSplitReader(),
            new ZeroMQRecordEmitter<>(deserializer), // Extracts byte[] -> OUT using DeserializationSchema
            readerContext.getConfiguration(),
            readerContext
        ) {
            @Override
            protected void onSplitFinished(Map<String, ZeroMQSourceSplit> map) {

            }

            @Override
            protected ZeroMQSourceSplit initializedState(ZeroMQSourceSplit zeroMQSourceSplit) {
                return zeroMQSourceSplit;
            }

            @Override
            protected ZeroMQSourceSplit toSplitType(String splitId, ZeroMQSourceSplit splitState) {
                // Reconstructs the split from its active tracking state during checkpoint restores
                return splitState;
            }
        };
    }

    @Override
    public SplitEnumerator<ZeroMQSourceSplit, Collection<ZeroMQSourceSplit>> createEnumerator(
        SplitEnumeratorContext<ZeroMQSourceSplit> enumContext) {
        return new ZeroMQSplitEnumerator(enumContext, targetConnections);
    }

    @Override
    public SplitEnumerator<ZeroMQSourceSplit, Collection<ZeroMQSourceSplit>> restoreEnumerator(SplitEnumeratorContext<ZeroMQSourceSplit> enumContext, Collection<ZeroMQSourceSplit> checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<ZeroMQSourceSplit> getSplitSerializer() {
        return new ZeroMQSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<ZeroMQSourceSplit>> getEnumeratorCheckpointSerializer() {
        return new ZeroMQEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializer.getProducedType();
    }
}

