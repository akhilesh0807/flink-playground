package com.example.flink.mocks;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class MockZMQPublisher {

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket publisher = context.createSocket(SocketType.PUB);
            // Bind to the port the Flink driver is listening to
            publisher.bind("tcp://*:5555");

            System.out.println("ZeroMQ Publisher bound to port 5555. Waiting 2 seconds for subscribers to connect...");
            Thread.sleep(2000);
            System.out.println("Streaming data...");

            int count = 0;
            while (!Thread.currentThread().isInterrupted()) {
                // Formatting payload with topic matching the Flink split definition
                String message = "tick: Temperature payload index " + count++;
                publisher.send(message.getBytes(ZMQ.CHARSET));

                Thread.sleep(1000); // Emit a message every 1 second
            }
        }
    }
}
