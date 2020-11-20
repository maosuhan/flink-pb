package org.apache.flink.pb;

import junit.framework.TestCase;
import org.apache.flink.pb.proto.OneofTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class RowToOneofProtoBytesTest extends TestCase {
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(1, 2);

        byte[] bytes = FlinkProtobufHelper.rowToPbBytes(row, OneofTest.class);
        OneofTest oneofTest = OneofTest.parseFrom(bytes);
        assertFalse(oneofTest.hasA());
        assertEquals(2, oneofTest.getB());
    }

}
