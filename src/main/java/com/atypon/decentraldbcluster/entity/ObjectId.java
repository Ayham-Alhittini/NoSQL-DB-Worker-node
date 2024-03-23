package com.atypon.decentraldbcluster.entity;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


public class ObjectId {
    private static final AtomicInteger counter = new AtomicInteger((int) (Math.random() * Integer.MAX_VALUE));
    private static final int machineIdentifier = (int) (Math.random() * Integer.MAX_VALUE) & 0xFFFFFF;
    private static final short processIdentifier = (short) (Math.random() * Short.MAX_VALUE);

    private final int timestamp;
    private final int machineAndProcessIdentifier;
    private final int counterValue;
    private static final char[] hexArray = "0123456789abcdef".toCharArray();

    public ObjectId() {
        this.timestamp = (int) (System.currentTimeMillis() / 1000);
        this.machineAndProcessIdentifier = (machineIdentifier << 16) | (processIdentifier & 0xFFFF);
        this.counterValue = counter.getAndIncrement();
    }

    public String toHexString() {
        byte[] bytes = ByteBuffer.allocate(12)
                .putInt(timestamp)
                .putInt(machineAndProcessIdentifier)
                .putInt(counterValue)
                .array();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
