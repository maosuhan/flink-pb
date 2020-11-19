package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.OneofTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class RowToOneofProtoBytesTest extends TestCase {
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(1, 2);

        RowType rowType = PbRowTypeInformation.generateRowType(OneofTest.getDescriptor());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
                rowType,
                OneofTest.class.getName());

        byte[] bytes = serializationSchema.serialize(row);
        OneofTest oneofTest = OneofTest.parseFrom(bytes);
        assertFalse(oneofTest.hasA());
        assertEquals(2, oneofTest.getB());
    }

}
