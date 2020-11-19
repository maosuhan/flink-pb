package org.apache.flink.pb;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class PbEncodingFormat implements EncodingFormat<SerializationSchema<RowData>> {
    private String messageClassName;

    public PbEncodingFormat(String messageClassName) {
        this.messageClassName = messageClassName;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType consumedDataType) {
        RowDataTypeInfo producedTypeInfo = (RowDataTypeInfo) context.createTypeInformation(consumedDataType);
        RowType rowType = producedTypeInfo.toRowType();
        return new PbRowSerializationSchema(rowType, this.messageClassName);
    }
}
