package org.apache.flink.pb;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.flink.pb.proto.SimpleTestNoouterNomultiOuterClass;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class NoouterNomultiProtoToRowTest extends TestCase {
    public void testSimple() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(SimpleTestNoouterNomultiOuterClass.SimpleTestNoouterNomulti.getDescriptor());
        PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(rowType, SimpleTestNoouterNomultiOuterClass.SimpleTestNoouterNomulti.class.getName());

        SimpleTestNoouterNomultiOuterClass.SimpleTestNoouterNomulti simple = SimpleTestNoouterNomultiOuterClass.SimpleTestNoouterNomulti.newBuilder()
                .setA(1)
                .setB(2L)
                .setC(false)
                .setD(0.1f)
                .setE(0.01)
                .setF("haha")
                .setG(ByteString.copyFrom(new byte[]{1}))
                .build();

        RowData row = deserializationSchema.deserialize(simple.toByteArray());
        row = FlinkProtobufHelper.validateRow(row, rowType);

        assertEquals(7, row.getArity());
        assertEquals(1, row.getInt(0));
        assertEquals(2L, row.getLong(1));
        assertFalse(row.getBoolean(2));
        assertEquals(0.1f, row.getFloat(3));
        assertEquals(0.01, row.getDouble(4));
        assertEquals("haha", row.getString(5).toString());
        assertEquals(1, (row.getBinary(6))[0]);
    }
}
