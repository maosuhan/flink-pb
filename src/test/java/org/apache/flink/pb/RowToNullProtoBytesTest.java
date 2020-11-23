package org.apache.flink.pb;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.pb.proto.NullTest;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.pb.FlinkProtobufHelper.mapOf;

public class RowToNullProtoBytesTest extends TestCase {
    /**
     * map<string, string> string_map = 1;
     * map<int32, int32> int_map = 2;
     * map<int64, int64> long_map = 3;
     * map<bool, bool> boolean_map = 4;
     * map<string, float> float_map = 5;
     * map<string, double> double_map = 6;
     * map<string, Corpus> enum_map = 7;
     * map<string, InnerMessageTest> message_map = 8;
     * <p>
     * repeated string string_array = 10;
     * repeated int32 int_array = 11;
     * repeated int64 long_array = 12;
     * repeated bool boolean_array = 13;
     * repeated float float_array = 14;
     * repeated double double_array = 15;
     * repeated Corpus enum_array = 16;
     * repeated InnerMessageTest message_array = 17;
     *
     * @throws Exception
     */
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(
                //string
                new GenericMapData(mapOf(null, StringData.fromString("value"), StringData.fromString("key"), null)),
                //int32
                new GenericMapData(mapOf(null, 1, 1, null)),
                //int64
                new GenericMapData(mapOf(null, 1L, 1L, null)),
                //boolean
                new GenericMapData(mapOf(null, true, true, null)),
                //float
                new GenericMapData(mapOf(StringData.fromString("key"), null)),
                //double
                new GenericMapData(mapOf(StringData.fromString("key"), null)),
                //enum
                new GenericMapData(mapOf(StringData.fromString("key"), null)),
                //message
                new GenericMapData(mapOf(StringData.fromString("key"), null)),
                //bytes
                new GenericMapData(mapOf(StringData.fromString("key"), null))
                ,
                //string
                new GenericArrayData(new Object[]{null}),
                //int
                new GenericArrayData(new Object[]{null}),
                //long
                new GenericArrayData(new Object[]{null}),
                //boolean
                new GenericArrayData(new Object[]{null}),
                //float
                new GenericArrayData(new Object[]{null}),
                //double
                new GenericArrayData(new Object[]{null}),
                //enum
                new GenericArrayData(new Object[]{null}),
                //message
                new GenericArrayData(new Object[]{null}),
                //bytes
                new GenericArrayData(new Object[]{null})
        );
        byte[] bytes = FlinkProtobufHelper.rowToPbBytes(row, NullTest.class);
        NullTest nullTest = NullTest.parseFrom(bytes);
        //string map
        assertEquals(2, nullTest.getStringMapCount());
        assertTrue(nullTest.getStringMapMap().containsKey(""));
        assertTrue(nullTest.getStringMapMap().containsKey("key"));
        assertEquals("value", nullTest.getStringMapMap().get(""));
        assertEquals("", nullTest.getStringMapMap().get("key"));
        //int32 map
        assertEquals(2, nullTest.getIntMapCount());
        assertTrue(nullTest.getIntMapMap().containsKey(0));
        assertTrue(nullTest.getIntMapMap().containsKey(1));
        assertEquals(Integer.valueOf(1), nullTest.getIntMapMap().get(0));
        assertEquals(Integer.valueOf(0), nullTest.getIntMapMap().get(1));
        //int64 map
        assertEquals(2, nullTest.getIntMapCount());
        assertTrue(nullTest.getLongMapMap().containsKey(0L));
        assertTrue(nullTest.getLongMapMap().containsKey(1L));
        assertEquals(Long.valueOf(1L), nullTest.getLongMapMap().get(0L));
        assertEquals(Long.valueOf(0L), nullTest.getLongMapMap().get(1L));
        //bool map
        assertEquals(2, nullTest.getBooleanMapCount());
        assertTrue(nullTest.getBooleanMapMap().containsKey(false));
        assertTrue(nullTest.getBooleanMapMap().containsKey(true));
        assertEquals(Boolean.TRUE, nullTest.getBooleanMapMap().get(false));
        assertEquals(Boolean.FALSE, nullTest.getBooleanMapMap().get(true));
        //float map
        assertEquals(1, nullTest.getFloatMapCount());
        assertEquals(Float.valueOf(0.0f), nullTest.getFloatMapMap().get("key"));
        //double map
        assertEquals(1, nullTest.getDoubleMapCount());
        assertEquals(Double.valueOf(0.0), nullTest.getDoubleMapMap().get("key"));
        //enum map
        assertEquals(1, nullTest.getEnumMapCount());
        assertEquals(NullTest.Corpus.UNIVERSAL, nullTest.getEnumMapMap().get("key"));
        //message map
        assertEquals(1, nullTest.getMessageMapCount());
        assertEquals(NullTest.InnerMessageTest.getDefaultInstance(), nullTest.getMessageMapMap().get("key"));
        //bytes map
        assertEquals(1, nullTest.getBytesMapCount());
        assertEquals(ByteString.EMPTY, nullTest.getBytesMapMap().get("key"));

        assertEquals(1, nullTest.getStringArrayCount());
        assertEquals("", nullTest.getStringArrayList().get(0));
        assertEquals(1, nullTest.getIntArrayCount());
        assertEquals(Integer.valueOf(0), nullTest.getIntArrayList().get(0));
        assertEquals(1, nullTest.getLongArrayCount());
        assertEquals(Long.valueOf(0L), nullTest.getLongArrayList().get(0));
        assertEquals(1, nullTest.getFloatArrayCount());
        assertEquals((float) 0, nullTest.getFloatArrayList().get(0));
        assertEquals(1, nullTest.getDoubleArrayCount());
        assertEquals((double) 0, nullTest.getDoubleArrayList().get(0));
        assertEquals(1, nullTest.getBooleanArrayCount());
        assertEquals(Boolean.FALSE, nullTest.getBooleanArrayList().get(0));
        assertEquals(1, nullTest.getEnumArrayCount());
        assertEquals(NullTest.Corpus.UNIVERSAL, nullTest.getEnumArrayList().get(0));
        assertEquals(1, nullTest.getMessageArrayCount());
        assertEquals(NullTest.InnerMessageTest.getDefaultInstance(), nullTest.getMessageArrayList().get(0));
        assertEquals(1, nullTest.getBytesArrayCount());
        assertEquals(ByteString.EMPTY, nullTest.getBytesArrayList().get(0));

    }
}
