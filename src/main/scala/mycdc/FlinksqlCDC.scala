package mycdc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object FlinksqlCDC {

  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1);

    val tableEnv = StreamTableEnvironment.create(env);

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

    val table = tableEnv.sqlQuery("select * from tb_src")

    //tableEnv.toDataStream(aliceVisitTable).print()
    val retractStream = tableEnv.toRetractStream[Row](table)

    retractStream.print()
    env.execute();

  }

}
