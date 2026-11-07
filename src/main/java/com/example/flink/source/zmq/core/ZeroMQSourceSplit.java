package com.example.flink.source.zmq.core;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/**
 * @param connectionString e.g., "tcp://localhost:5555"
 * @param topic            Used for PUB/SUB
 */
public record ZeroMQSourceSplit(String splitId, String connectionString,
                                String topic) implements SourceSplit, Serializable {
}
