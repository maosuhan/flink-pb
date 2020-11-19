package org.apache.flink.pb;

import java.util.concurrent.atomic.AtomicInteger;

public class CodegenVarUid {
    private static CodegenVarUid varUid = new CodegenVarUid();
    private AtomicInteger atomicInteger = new AtomicInteger();

    private CodegenVarUid() {
    }

    public static CodegenVarUid getInstance() {
        return varUid;
    }

    public int getAndIncrement() {
        return atomicInteger.getAndIncrement();
    }
}
