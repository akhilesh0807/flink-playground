package com.example.flink.source.zmq.core;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.*;

public class ZeroMQSplitEnumerator implements SplitEnumerator<ZeroMQSourceSplit, Collection<ZeroMQSourceSplit>> {

    private final SplitEnumeratorContext<ZeroMQSourceSplit> context;
    private final Queue<ZeroMQSourceSplit> unassignedSplits;

    public ZeroMQSplitEnumerator(SplitEnumeratorContext<ZeroMQSourceSplit> context, List<ZeroMQSourceSplit> splits) {
        this.context = context;
        this.unassignedSplits = new ArrayDeque<>(splits);
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!unassignedSplits.isEmpty()) {
            context.assignSplit(unassignedSplits.poll(), subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId) { // Handle late-joining TaskManagers
        if (!unassignedSplits.isEmpty()) {
            context.assignSplit(unassignedSplits.poll(), subtaskId);
        }
    }

    @Override public void start() {}
    @Override public void addSplitsBack(List<ZeroMQSourceSplit> splits, int subtaskId) { unassignedSplits.addAll(splits); }
    @Override public Collection<ZeroMQSourceSplit> snapshotState(long checkpointId) { return new ArrayList<>(unassignedSplits); }
    @Override public void close() {}
}
