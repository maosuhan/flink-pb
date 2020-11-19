package org.apache.flink.pb.starter;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SimpleProtobufTableSink implements DynamicTableSink {
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private DataType producedDataType;

    public SimpleProtobufTableSink(EncodingFormat<SerializationSchema<RowData>> decodingFormat, DataType dataType) {
        this.encodingFormat = decodingFormat;
        this.producedDataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(
                context,
                producedDataType);
        return new SinkFunctionProvider() {

            @Override
            public SinkFunction<RowData> createSinkFunction() {
                return new SimpleProtobufSinkFunction(serializer);
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new SimpleProtobufTableSink(encodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return SimpleProtobufTableSink.class.getName();
    }

}
