package com.example.flink.source.zmq.utils;

import com.example.flink.source.zmq.core.ZeroMQSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;
import java.util.*;

public class ZeroMQEnumStateSerializer implements SimpleVersionedSerializer<Collection<ZeroMQSourceSplit>> {
    private static final int CURRENT_VERSION = 1;
    private final ZeroMQSplitSerializer splitSerializer = new ZeroMQSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<ZeroMQSourceSplit> splits) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(splits.size());
            for (ZeroMQSourceSplit split : splits) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                dos.writeInt(serializedSplit.length);
                dos.write(serializedSplit);
            }
            return baos.toByteArray();
        }
    }


    @Override
    public Collection<ZeroMQSourceSplit> deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            int size = dis.readInt();
            List<ZeroMQSourceSplit> splits = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int length = dis.readInt();
                byte[] serializedSplit = new byte[length];
                dis.readFully(serializedSplit);
                splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), serializedSplit));
            }
            return splits;
        }
    }
}
