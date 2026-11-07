package com.example.flink.source.zmq.utils;

import com.example.flink.source.zmq.core.ZeroMQSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;

public class ZeroMQSplitSerializer implements SimpleVersionedSerializer<ZeroMQSourceSplit> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ZeroMQSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(split.splitId());
            dos.writeUTF(split.connectionString());
            dos.writeUTF(split.topic());
            return baos.toByteArray();
        }
    }

    @Override
    public ZeroMQSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            String splitId = dis.readUTF();
            String connectionString = dis.readUTF();
            String topic = dis.readUTF();
            return new ZeroMQSourceSplit(splitId, connectionString, topic);
        }
    }
}

