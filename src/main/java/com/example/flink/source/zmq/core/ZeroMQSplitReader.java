package com.example.flink.source.zmq.core;

import com.example.flink.source.zmq.dto.ZeroMQRecords;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.*;


public class ZeroMQSplitReader implements SplitReader<byte[], ZeroMQSourceSplit>{
    private final ZContext zmqContext = new ZContext();
    private final Map<String, ZMQ.Socket> activeSockets = new HashMap<>();
    private final Queue<ZeroMQSourceSplit> pendingSplits = new ArrayDeque<>();
    private ZMQ.Poller poller;

    @Override
    public RecordsWithSplitIds<byte[]> fetch() throws IOException {
        boolean newSplitsAdded = false;

        // 1. Initialize any new splits assigned to us
        while (!pendingSplits.isEmpty()) {
            ZeroMQSourceSplit split = pendingSplits.poll();
            ZMQ.Socket socket = zmqContext.createSocket(SocketType.SUB);
            socket.connect(split.connectionString());
            socket.subscribe(split.topic().getBytes(ZMQ.CHARSET));
            activeSockets.put(split.splitId(), socket);
            newSplitsAdded = true;
        }

        ZeroMQRecords<byte[]> outputBatch = new ZeroMQRecords<>();

        if (activeSockets.isEmpty()) {
            // No active sockets yet, sleep briefly to avoid busy waiting
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return outputBatch;
        }

        // 2. Initialize or rebuild poller if new splits were added or poller is null
        if (poller == null || newSplitsAdded) {
            if (poller != null) {
                poller.close();
            }
            poller = zmqContext.createPoller(activeSockets.size());
            for (ZMQ.Socket socket : activeSockets.values()) {
                poller.register(socket, ZMQ.Poller.POLLIN);
            }
        }

        // 3. Poll active sockets with a 200ms timeout
        int events = poller.poll(200);
        if (events > 0) {
            List<String> splitIds = new ArrayList<>(activeSockets.keySet());
            for (int i = 0; i < splitIds.size(); i++) {
                if (poller.pollin(i)) {
                    String splitId = splitIds.get(i);
                    ZMQ.Socket socket = activeSockets.get(splitId);
                    byte[] message = socket.recv(ZMQ.DONTWAIT);
                    System.out.println("Received bytes on split " + splitId + ": " + (message != null ? new String(message, ZMQ.CHARSET) : "null"));
                    if (message != null) {
                        outputBatch.addRecord(splitId, message);
                    }
                }
            }
        }

        return outputBatch;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<ZeroMQSourceSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            pendingSplits.addAll(splitsChange.splits());
        }
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() {
        if (poller != null) {
            poller.close();
        }
        for (ZMQ.Socket socket : activeSockets.values()) {
            socket.close();
        }
        zmqContext.close();
    }
}
