package org.apache.flink.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class PbDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    private String messageClassName;
    private boolean ignoreParseErrors;
    private boolean ignoreDefaultValues;

    public PbDecodingFormat(String messageClassName, boolean ignoreParseErrors, boolean ignoreDefaultValues) {
        this.messageClassName = messageClassName;
        this.ignoreParseErrors = ignoreParseErrors;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        RowDataTypeInfo rowDataTypeInfo = (RowDataTypeInfo) context.createTypeInformation(producedDataType);
        RowType rowtype = rowDataTypeInfo.toRowType();
        return new PbRowDeserializationSchema(rowtype, this.messageClassName, this.ignoreParseErrors,this.ignoreDefaultValues);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
