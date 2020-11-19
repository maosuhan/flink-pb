package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.RepeatedMessageTest;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class RepeatedMessageProtoToRowTest extends TestCase {
    public void testRepeatedMessage() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(RepeatedMessageTest.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType, RepeatedMessageTest.class.getName());

        RepeatedMessageTest.InnerMessageTest innerMessageTest = RepeatedMessageTest.InnerMessageTest.newBuilder()
                .setA(1)
                .setB(2L)
                .build();

        RepeatedMessageTest.InnerMessageTest innerMessageTest1 = RepeatedMessageTest.InnerMessageTest.newBuilder()
                .setA(3)
                .setB(4L)
                .build();

        RepeatedMessageTest repeatedMessageTest = RepeatedMessageTest.newBuilder()
                .addD(innerMessageTest)
                .addD(innerMessageTest1)
                .build();

        RowData row = deserializationSchema.deserialize(repeatedMessageTest.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        ArrayData objs = row.getArray(0);
        RowData subRow = objs.getRow(0, 2);
        assertEquals(1, subRow.getInt(0));
        assertEquals(2L, subRow.getLong(1));
        subRow = objs.getRow(1, 2);
        assertEquals(3, subRow.getInt(0));
        assertEquals(4L, subRow.getLong(1));

    }
}
