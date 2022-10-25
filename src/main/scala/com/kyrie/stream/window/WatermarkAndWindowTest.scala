package com.kyrie.stream.window

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 水位线和时间窗口测试-统计每个用户的活跃度
 */
object WatermarkAndWindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.socketTextStream("localhost",9090)
      .map(line => {
        val Array(user,url,ts) = line.split(",")
        Event(user,url,ts.toLong)
      })
      //乱序
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
          .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
          })
      )
      .keyBy(_.user)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new WatermarkWindowTest)
      .print()

    env.execute()

  }

  class WatermarkWindowTest extends ProcessWindowFunction[Event,String,String,TimeWindow]{
    //每个窗口执行一次
    override def process(user: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      println("xx:")
      val count = elements.size
      val watermark = context.currentWatermark
      val start = context.window.getStart
      val end = context.window.getEnd
      out.collect(s"用户：${user}, 窗口：${start}~${end},活跃度：${count},当期水位线：${watermark}")
    }
  }
}
