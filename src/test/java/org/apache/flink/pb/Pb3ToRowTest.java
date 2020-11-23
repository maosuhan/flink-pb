package org.apache.flink.pb;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.flink.pb.proto.Pb3Test;
import org.apache.flink.pb.proto.Pb3Test.Corpus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class Pb3ToRowTest extends TestCase {
    public void testMessage() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(Pb3Test.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType,
                Pb3Test.class.getName(), false, false);

        Pb3Test.InnerMessageTest innerMessageTest = Pb3Test.InnerMessageTest.newBuilder().setA(1).setB(2).build();
        Pb3Test mapTest = Pb3Test.newBuilder()
                .setA(1)
                .setB(2L)
                .setC("haha")
                .setD(1.1f)
                .setE(1.2)
                .setF(Corpus.IMAGES)
                .setG(innerMessageTest)
                .addH(innerMessageTest)
                .setI(ByteString.copyFrom(new byte[]{100}))
                .putMap1("a", "b")
                .putMap1("c", "d")
                .putMap2("f", innerMessageTest).build();

        RowData row = deserializationSchema.deserialize(mapTest.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        assertEquals(1, row.getInt(0));
        assertEquals(2L, row.getLong(1));
        assertEquals("haha", row.getString(2).toString());
        assertEquals(1.1f, row.getFloat(3));
        assertEquals(1.2, row.getDouble(4));
        assertEquals("IMAGES", row.getString(5).toString());

        RowData rowData = row.getRow(6, 2);
        assertEquals(1, rowData.getInt(0));
        assertEquals(2L, rowData.getInt(1));

        rowData = row.getArray(7).getRow(0, 2);
        assertEquals(1, rowData.getInt(0));
        assertEquals(2L, rowData.getInt(1));

        assertEquals(100, row.getBinary(8)[0]);

        MapData map1 = row.getMap(9);
        assertEquals("a", map1.keyArray().getString(0).toString());
        assertEquals("b", map1.valueArray().getString(0).toString());
        assertEquals("c", map1.keyArray().getString(1).toString());
        assertEquals("d", map1.valueArray().getString(1).toString());

        MapData map2 = row.getMap(10);
        assertEquals("f", map2.keyArray().getString(0).toString());
        rowData = map2.valueArray().getRow(0, 2);

        assertEquals(1, rowData.getInt(0));
        assertEquals(2L, rowData.getLong(1));
    }

    public void testDefaultValues() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(Pb3Test.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType,
                Pb3Test.class.getName(), false, false);

        Pb3Test mapTest = Pb3Test.newBuilder().build();

        RowData row = deserializationSchema.deserialize(mapTest.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        assertFalse(row.isNullAt(0));
        assertFalse(row.isNullAt(1));
        assertFalse(row.isNullAt(2));
        assertFalse(row.isNullAt(3));
        assertFalse(row.isNullAt(4));
        assertFalse(row.isNullAt(5));
        assertFalse(row.isNullAt(6));
        assertFalse(row.isNullAt(7));
        assertFalse(row.isNullAt(8));
        assertFalse(row.isNullAt(9));
        assertFalse(row.isNullAt(10));

        assertEquals(0, row.getInt(0));
        assertEquals(0L, row.getLong(1));
        assertEquals("", row.getString(2).toString());
        assertEquals(0.0f, row.getFloat(3));
        assertEquals(0.0, row.getDouble(4));
        assertEquals("UNIVERSAL", row.getString(5).toString());

        RowData rowData = row.getRow(6, 2);
        assertEquals(0, rowData.getInt(0));
        assertEquals(0L, rowData.getLong(1));

        assertEquals(0, row.getArray(7).size());

        assertEquals(0, row.getBinary(8).length);

        assertEquals(0, row.getMap(9).size());
        assertEquals(0, row.getMap(10).size());
    }

}
