package com.kyrie.stream.window

import com.kyrie.stream.source.ClickSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 增量聚合函数-归约函数
 */
object WindowReduceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .map(r => r.user ->1)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((r1,r2) => (r1._1, r1._2 +r2._2))
      .print()

    env.execute()
  }

}
