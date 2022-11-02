package mycdc

import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema
import org.apache.flink.streaming.api.scala._

object FlinkCDCTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1);

    val mysqlSource = MySqlSource.builder[String]
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("11111111")
      .databaseList("test")
      .tableList("test.tb_src")
      .serverTimeZone("Asia/Shanghai")
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDebeziumDeserializationSchema())
      .build()

    //4.使用CDC Source从MySQL读取数据
    val mysqlDS = env.addSource(mysqlSource)
    //5.打印数据
    mysqlDS.print
    //6.执行任务
    env.execute

  }

}
