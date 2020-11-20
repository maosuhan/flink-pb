package org.apache.flink.pb;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FlinkProtobufHelper {
    public static RowData validateRow(RowData row, RowType rowType) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        DataStream<RowData> rows = env.fromCollection(Collections.singletonList(row), new RowDataTypeInfo(rowType));
        Table table = tableEnv.fromDataStream(rows);
        tableEnv.registerTable("t", table);
        table = tableEnv.sqlQuery("select * from t");
        DataStream newRows = tableEnv.toAppendStream(table, new RowDataTypeInfo(rowType));
        return (RowData) DataStreamUtils.collect(newRows).next();
    }

    public static byte[] rowToPbBytes(RowData row, Class messageClass) throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(PbDesSerUtils.getDescriptor(messageClass.getName()));
        row = validateRow(row, rowType);
        PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(rowType,
                messageClass.getName());
        byte[] bytes = serializationSchema.serialize(row);
        return bytes;
    }

    public static <K, V> Map<K, V> mapOf(Object... keyValues) {
        Map<K, V> map = new HashMap<>();

        for (int index = 0; index < keyValues.length / 2; index++) {
            map.put((K) keyValues[index * 2], (V) keyValues[index * 2 + 1]);
        }

        return map;
    }

}
