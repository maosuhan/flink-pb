package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.MapTest;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.Map;

public class RowToMapProtoBytesTest extends TestCase {
    public void testSimple() throws Exception {
        Map map1 = new HashMap();
        map1.put(StringData.fromString("a"), StringData.fromString("b"));
        Map map2 = new HashMap();
        map2.put(StringData.fromString("c"), GenericRowData.of(1, 2L));
        RowData row = GenericRowData.of(1, new GenericMapData(map1), new GenericMapData(map2));

        RowType rowType = PbRowTypeInformation.generateRowType(MapTest.getDescriptor());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(rowType,
                MapTest.class.getName());

        byte[] bytes = serializationSchema.serialize(row);
        MapTest mapTest = MapTest.parseFrom(bytes);
        assertEquals(1, mapTest.getA());
        assertEquals("b", mapTest.getMap1Map().get("a"));
        MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("c");
        assertEquals(1, innerMessageTest.getA());
        assertEquals(2L, innerMessageTest.getB());
    }
}
