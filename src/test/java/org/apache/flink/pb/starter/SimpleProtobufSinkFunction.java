package org.apache.flink.pb.starter;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.pb.proto.SimpleTest;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

public class SimpleProtobufSinkFunction extends RichSinkFunction<RowData> {
    private SerializationSchema<RowData> serializer;

    public SimpleProtobufSinkFunction(SerializationSchema<RowData> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        byte[] bytes = this.serializer.serialize(value);
        SimpleTest simpleTest = SimpleTest.parseFrom(bytes);
        simpleTest.hasA();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.serializer.open(() -> null);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
