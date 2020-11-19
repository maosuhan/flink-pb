package org.apache.flink.pb.starter;

import com.google.protobuf.ByteString;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.pb.proto.SimpleTest;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;


public class SimpleProtobufSourceFunction extends RichSourceFunction<RowData> {
    private DeserializationSchema<RowData> deserializer;
    private byte[] simpleTestBytes;

    public SimpleProtobufSourceFunction(DeserializationSchema<RowData> deserializer) {
        this.deserializer = deserializer;
        SimpleTest simpleTest = SimpleTest.newBuilder()
                .setA(1)
                .setB(2L)
                .setC(true)
                .setD(1.0f)
                .setE(2.0)
                .setF("hahaha")
                .setG(ByteString.copyFrom(new byte[]{100}))
                .setH(SimpleTest.Corpus.UNIVERSAL)
                .build();
        this.simpleTestBytes = simpleTest.toByteArray();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        deserializer.open(() -> null);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (true) {
            RowData rowData = deserializer.deserialize(simpleTestBytes);
            ctx.collect(rowData);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}

