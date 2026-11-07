package com.example.flink.source.zmq.dto;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import java.util.*;

public class ZeroMQRecords<E> implements RecordsWithSplitIds<E> {

    // Maps a split ID to its collected records in this batch fetch cycle
    private final Map<String, List<E>> recordsBySplit = new HashMap<>();
    private final Set<String> finishedSplits = new HashSet<>();

    private Iterator<Map.Entry<String, List<E>>> splitIterator;
    private Map.Entry<String, List<E>> currentSplitEntry;
    private Iterator<E> currentRecordIterator;

    /**
     * Helper method used by ZeroMQSplitReader to append data.
     */
    public void addRecord(String splitId, E record) {
        recordsBySplit.computeIfAbsent(splitId, k -> new ArrayList<>()).add(record);
    }


    @Override
    public String nextSplit() {
        // Initialize the split iterator on the first call
        if (splitIterator == null) {
            splitIterator = recordsBySplit.entrySet().iterator();
        }

        if (splitIterator.hasNext()) {
            currentSplitEntry = splitIterator.next();
            currentRecordIterator = currentSplitEntry.getValue().iterator();
            return currentSplitEntry.getKey();
        }

        // No more splits containing records in this batch cycle
        currentSplitEntry = null;
        currentRecordIterator = null;
        return null;
    }

    @Override
    public E nextRecordFromSplit() {
        if (currentRecordIterator != null && currentRecordIterator.hasNext()) {
            return currentRecordIterator.next();
        }
        return null;
    }

    @Override
    public Set<String> finishedSplits() {
        // ZeroMQ is an unbounded streaming source, so connections typically never "finish"
        return finishedSplits;
    }

    @Override
    public void recycle() {
        // Clear references to free up batch resources safely
        recordsBySplit.clear();
        finishedSplits.clear();
        splitIterator = null;
        currentSplitEntry = null;
        currentRecordIterator = null;
    }
}