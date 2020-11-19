package org.apache.flink.pb;

import java.util.concurrent.atomic.AtomicInteger;

public class VarUid {
    private static VarUid varUid = new VarUid();
    private AtomicInteger atomicInteger = new AtomicInteger();

    private VarUid() {
    }

    public static VarUid getInstance() {
        return varUid;
    }

    public int getAndIncrement() {
        return atomicInteger.getAndIncrement();
    }
}
