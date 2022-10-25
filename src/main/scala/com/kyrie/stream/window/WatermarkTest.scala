package com.kyrie.stream.window

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration

/**
 * 水位线分为：事件，时间周期的
 * 时间周期分为：单增，乱序
 */
object WatermarkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new ClickSource)

    //针对乱序流插入水位线，延迟时间设置为 5s
    stream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event]{
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        })

    ).print()

    env.execute()
  }


}
