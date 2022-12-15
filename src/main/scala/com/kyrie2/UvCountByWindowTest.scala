package com.kyrie2

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object UvCountByWindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_ => "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new ProcessWindowFunction[Event,String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Event],
                             out: Collector[String]): Unit = {
          var userSet = Set[String]()

          elements.foreach(userSet += _.user)
          val start = context.window.getStart
          val end = context.window.getEnd

          out.collect(" 窗 口 ： " + new Timestamp(start) + "~" + new
              Timestamp(end) + "的独立访客数量是： " + userSet.size)
        }
      }).print()

    env.execute()

  }

}
