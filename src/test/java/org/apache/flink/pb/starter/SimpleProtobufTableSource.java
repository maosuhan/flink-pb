package org.apache.flink.pb.starter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SimpleProtobufTableSource implements ScanTableSource {
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType producedDataType;

    public SimpleProtobufTableSource(DecodingFormat<DeserializationSchema<RowData>> decodingFormat, DataType dataType) {
        this.decodingFormat = decodingFormat;
        this.producedDataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                runtimeProviderContext,
                producedDataType);

        return new SourceFunctionProvider() {
            @Override
            public boolean isBounded() {
                return false;
            }

            @Override
            public SourceFunction<RowData> createSourceFunction() {
                return new SimpleProtobufSourceFunction(deserializer);
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new SimpleProtobufTableSource(decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return SimpleProtobufTableSource.class.getName();
    }
}
