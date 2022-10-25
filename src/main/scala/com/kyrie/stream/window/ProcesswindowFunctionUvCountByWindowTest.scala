package com.kyrie.stream.window

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * 处理窗口函数-计算窗口内uv数量
 */
object ProcesswindowFunctionUvCountByWindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_=> "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UvCountByWindow)
      .print()

  env.execute()
  }

  class UvCountByWindow extends ProcessWindowFunction[Event,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {

      var userSet = Set[String]()
      elements.foreach(userSet +=_.user)

      val windowStart = context.window.getStart

      val windowEnd = context.window.getEnd

      out.collect(" 窗 口 ： " + new Timestamp(windowStart) + "~" + new
          Timestamp(windowEnd) + "的独立访客数量是： " + userSet.size)

    }
  }
}
