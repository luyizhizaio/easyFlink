package com.kyrie.example;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {

  public static void main(String[] args) throws Exception {

    //1.创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    //2.创建Flink-MySQL-CDC的Source
    tableEnv.executeSql("CREATE TABLE tb_src (" +
      "  id BIGINT," +
      "  name STRING," +
      "  createdate TIMESTAMP(0)," +
      "PRIMARY KEY(id) NOT ENFORCED"+
      ") WITH (" +
      "  'connector' = 'mysql-cdc'," +
      "  'hostname' = 'localhost'," +
      "  'server-time-zone' = 'Asia/Shanghai'," +
      "  'scan.startup.mode' = 'latest-offset'," +
      "  'port' = '3306'," +
      "  'username' = 'root'," +
      "  'password' = '11111111'," +
      "  'database-name' = 'test'," +
      "  'table-name' = 'tb_src'" +
      ")");

    Table table = tableEnv.sqlQuery("select * from tb_src");

    DataStream<Tuple2<Boolean, Row>> stream = tableEnv.toRetractStream(table, Row.class);

    stream.print();

    env.execute();
  }

}
