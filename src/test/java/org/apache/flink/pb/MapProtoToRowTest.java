package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.MapTest;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class MapProtoToRowTest extends TestCase {
    public void testMessage() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(MapTest.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType,
                MapTest.class.getName());

        MapTest.InnerMessageTest innerMessageTest = MapTest.InnerMessageTest.newBuilder().setA(1).setB(2).build();
        MapTest mapTest = MapTest.newBuilder()
                .setA(1)
                .putMap1("a", "b")
                .putMap1("c", "d")
                .putMap2("f", innerMessageTest).build();

        RowData row = deserializationSchema.deserialize(mapTest.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        MapData map1 = row.getMap(1);
        assertEquals("a", map1.keyArray().getString(0).toString());
        assertEquals("b", map1.valueArray().getString(0).toString());
        assertEquals("c", map1.keyArray().getString(1).toString());
        assertEquals("d", map1.valueArray().getString(1).toString());

        MapData map2 = row.getMap(2);
        assertEquals("f", map2.keyArray().getString(0).toString());
        RowData rowData2 = map2.valueArray().getRow(0, 2);

        assertEquals(1, rowData2.getInt(0));
        assertEquals(2L, rowData2.getLong(1));
    }
}
