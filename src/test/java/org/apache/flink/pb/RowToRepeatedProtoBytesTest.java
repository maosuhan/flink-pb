package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.RepeatedTest;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

public class RowToRepeatedProtoBytesTest extends TestCase {
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(1, new GenericArrayData(new Object[]{1L, 2L, 3L}), false, 0.1f, 0.01, StringData.fromString("hello"));

        RowType rowType = PbRowTypeInformation.generateRowType(RepeatedTest.getDescriptor());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
                rowType,
                RepeatedTest.class.getName());

        byte[] bytes = serializationSchema.serialize(row);
        RepeatedTest repeatedTest = RepeatedTest.parseFrom(bytes);
        assertEquals(3, repeatedTest.getBCount());
        assertEquals(1L, repeatedTest.getB(0));
        assertEquals(2L, repeatedTest.getB(1));
        assertEquals(3L, repeatedTest.getB(2));
    }

    public void testEmptyArray() throws Exception {
        RowData row = GenericRowData.of(1, new GenericArrayData(new Object[]{}), false, 0.1f, 0.01, StringData.fromString("hello"));

        RowType rowType = PbRowTypeInformation.generateRowType(RepeatedTest.getDescriptor());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
                rowType,
                RepeatedTest.class.getName());

        byte[] bytes = serializationSchema.serialize(row);
        RepeatedTest repeatedTest = RepeatedTest.parseFrom(bytes);
        assertEquals(0, repeatedTest.getBCount());
    }

}
