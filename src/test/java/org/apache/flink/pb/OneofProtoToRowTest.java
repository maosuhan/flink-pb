package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.OneofTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class OneofProtoToRowTest extends TestCase {
    public void testSimple() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(OneofTest.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType, OneofTest.class.getName());

        OneofTest oneofTest = OneofTest.newBuilder()
                .setA(1)
                .setB(2)
                .build();

        RowData row = deserializationSchema.deserialize(oneofTest.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        assertTrue(row.isNullAt(0));
        assertEquals(2, row.getInt(1));
    }

}
