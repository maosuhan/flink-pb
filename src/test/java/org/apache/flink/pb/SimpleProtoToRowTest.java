package org.apache.flink.pb;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.flink.pb.proto.SimpleTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class SimpleProtoToRowTest extends TestCase {
    public void testSimple() throws Exception {
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(PbRowTypeInformation.generateRowType(SimpleTest.getDescriptor()), SimpleTest.class.getName());

        SimpleTest simple = SimpleTest.newBuilder()
                .setA(1)
                .setB(2L)
                .setC(false)
                .setD(0.1f)
                .setE(0.01)
                .setF("haha")
                .setG(ByteString.copyFrom(new byte[]{1}))
                .setH(SimpleTest.Corpus.IMAGES)
                .build();

        RowData row = deserializationSchema.deserialize(simple.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, PbRowTypeInformation.generateRowType(SimpleTest.getDescriptor()));

        assertEquals(8, row.getArity());
        assertEquals(1, row.getInt(0));
        assertEquals(2L, row.getLong(1));
        assertFalse((boolean) row.getBoolean(2));
        assertEquals(0.1f, row.getFloat(3));
        assertEquals(0.01, row.getDouble(4));
        assertEquals("haha", row.getString(5).toString());
        assertEquals(1, (row.getBinary(6))[0]);
        assertEquals("IMAGES", row.getString(7).toString());
    }

    public void testNotExistsValue() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(SimpleTest.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType, SimpleTest.class.getName());

        SimpleTest simple = SimpleTest.newBuilder()
                .setB(2L)
                .setC(false)
                .setD(0.1f)
                .setE(0.01)
                .setF("haha")
                .build();

        RowData row = deserializationSchema.deserialize(simple.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        assertTrue(row.isNullAt(0));
        assertFalse(row.isNullAt(1));
    }

    public void testDefaultValues() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(SimpleTest.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType, SimpleTest.class.getName(), false, false);

        SimpleTest simple = SimpleTest.newBuilder()
                .setC(false)
                .setD(0.1f)
                .setE(0.01)
                .build();

        RowData row = deserializationSchema.deserialize(simple.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        assertFalse(row.isNullAt(0));
        assertFalse(row.isNullAt(1));
        assertFalse(row.isNullAt(5));
        assertEquals(10, row.getInt(0));
        assertEquals(100L, row.getLong(1));
        assertEquals("f", row.getString(5).toString());
    }

}
