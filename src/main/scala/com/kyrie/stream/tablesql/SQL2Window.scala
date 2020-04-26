package com.kyrie.stream.tablesql

import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
/**
 * table 窗口聚合操作
 */
object SQL2Window {

  case class Feedback3(id:String,timestamp:Long,fre:Int)

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.socketTextStream("192.168.0.104",9999,'\n')

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val feedbackStream = stream.map{line =>
      val Array(id,ts,num) = line.split(",")
      Feedback3(id,ts.toLong,num.toInt)
    }.keyBy(_.id)

    feedbackStream.print("input:")

    //添加watermark
    val watermarkStream = feedbackStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Feedback3](Time.seconds(5L)) {
      override def extractTimestamp(element: Feedback3): Long = element.timestamp * 1000
    })

    //stream转table; rowtime 表示eventtime
    val table :Table= tableEnv.fromDataStream[Feedback3](watermarkStream,'id,'ts.rowtime,'fre)

    //滚动窗口
   // val resultTable = table.window(Tumble over 10.second on 'ts as 'tt ).groupBy('id,'tt).select('id,'id.count)

    //滑动窗口
    //注意：如果使用的 api 包括时间窗口，那么窗口的字段必须出现在 groupBy 中。
    val resultTable = table.window(Slide over 10.second every  5.second on 'ts as 'tt )
      .groupBy('id,'tt)
      .select('id,'id.count)

    val resultStream = tableEnv.toAppendStream[(String,Long)](resultTable)

    resultStream.print("result")


    env.execute()

  }



}
