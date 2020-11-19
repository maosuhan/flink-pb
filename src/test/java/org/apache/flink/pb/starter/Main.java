package org.apache.flink.pb.starter;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(8);
        streamEnv.enableCheckpointing(1000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        String sql = "create table source(a int, b bigint, d float, e double, f string, g binary, h string) " +
                "with(" +
                "'connector' = 'simple-protobuf-source',\n" +
                " 'format' = 'pb',\n" +
                "'pb.message-class-name' = 'org.apache.flink.pb.proto.SimpleTest'\n" +
                ")";
        tableEnv.executeSql(sql);

        sql = "create table sink(a int, b bigint, d float, e double, f string, g binary, h string) " +
                "with(" +
                "'connector' = 'simple-protobuf-sink',\n" +
                " 'format' = 'pb',\n" +
                "'pb.message-class-name' = 'org.apache.flink.pb.proto.SimpleTest'\n" +
                ")";
        tableEnv.executeSql(sql);
        tableEnv.executeSql("insert into sink select * from source");
        tableEnv.execute("job");
    }
}
