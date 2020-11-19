package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.SimpleTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

public class RowToSimpleProtoBytesTest extends TestCase {
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(1, 2L, false, 0.1f, 0.01, StringData.fromString("hello"), new byte[]{1}, StringData.fromString("IMAGES"));

        RowType rowType = PbRowTypeInformation.generateRowType(SimpleTest.getDescriptor());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
                rowType,
                SimpleTest.class.getName());

        byte[] bytes = serializationSchema.serialize(row);
        SimpleTest simpleTest = SimpleTest.parseFrom(bytes);
        assertTrue(simpleTest.hasA());
        assertEquals(1, simpleTest.getA());
        assertEquals(2L, simpleTest.getB());
        assertFalse(simpleTest.getC());
        assertEquals(0.1f, simpleTest.getD());
        assertEquals(0.01, simpleTest.getE());
        assertEquals("hello", simpleTest.getF());
        assertEquals(1, simpleTest.getG().byteAt(0));
        assertEquals(SimpleTest.Corpus.IMAGES, simpleTest.getH());
    }

    public void testNull() throws Exception {
        RowData row = GenericRowData.of(null, 2L, false, 0.1f, 0.01, StringData.fromString("hello"), null, null);

        RowType rowtype = PbRowTypeInformation.generateRowType(SimpleTest.getDescriptor());
        row = FlinkProtobufHelper.validateRow(row, rowtype);

        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
                rowtype,
                SimpleTest.class.getName());

        byte[] bytes = serializationSchema.serialize(row);
        SimpleTest simpleTest = SimpleTest.parseFrom(bytes);
        assertFalse(simpleTest.hasA());
        assertFalse(simpleTest.hasG());
        assertFalse(simpleTest.hasH());
    }
}
