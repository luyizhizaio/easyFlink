package com.kyrie.stream.window


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 案例：每隔 5 秒统计最近 10 秒，每个基站的呼叫数量。
 * 要求： 1、每个基站的数据会存在乱序
 * 2、大多数数据延迟 2 秒到，但是有些数据迟到时间比较长,设置allowedLateness为5秒
 * 3、迟到时间超过5秒的数据不能丢弃，放入侧流
 */

case class StationLog(id:String,eventType:String,eventTime:Long)

object Window2AllowedLateness {

  //迟到数据侧输出流标签
  val lateTag = new OutputTag[StationLog]("lateData")

  def main(args: Array[String]): Unit = {



    val  env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("192.168.0.104",8888)
      .map{line =>
        val arr = line.split(",")
        StationLog(arr(0).trim,arr(1).trim,arr(2).trim.toLong)
      }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(2)) {
        override def extractTimestamp(element: StationLog): Long = element.eventTime
      })

    //1.迟到2秒以内的数据有watermark处理
    //2.迟到超过2秒的数据交给allowedLateness处理，分两种情况：
    //第一种情况：允许数据迟到5秒（迟到2-5秒且不包括5秒），再次延迟触发窗口函数。触发条件：watermark < end-of-window + allowedLateness
    //即 eventTime < end-of-window + watermark延时 + allowedLateness；就会触发窗口函数
    //第二种情况：延迟超过5秒的数据，输出到侧输出流

    val resultStream = stream.keyBy(_.id)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .allowedLateness(Time.seconds(4)) //运行数据迟到5秒，还可触发窗口
      .sideOutputLateData(lateTag)
      .aggregate(new CountAgg ,new ResultWindowFunction)

    val lateStream = resultStream.getSideOutput(lateTag)
    lateStream.print("late")

    resultStream.print("main")

    env.execute()
    /**测试数据与测试结果：
id1,suc,1577080478000
id1,suc,1577080479000
id1,suc,1577080468000
id1,suc,1577080481000
id1,suc,1577080469000

     main> 窗口范围：1577080465000----1577080475000
    设备id：id1sum:1
    late> StationLog(id1,suc,1577080469000)

     */


  }

  class CountAgg extends AggregateFunction[StationLog,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: StationLog, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+ acc1
  }

  class ResultWindowFunction extends WindowFunction[Long,String,String,TimeWindow]{
    override
    def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
      val builder = new StringBuilder

      val sum = input.iterator.next()

      builder.append("窗口范围：").append(window.getStart).append("----").append(window.getEnd).append("\n")
      builder.append("设备id：").append(key).append("sum:").append(sum)

      out.collect(builder.toString())


    }

  }

}
