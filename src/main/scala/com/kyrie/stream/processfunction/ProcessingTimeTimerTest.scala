package com.kyrie.stream.processfunction

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * 测试定时器
 */
object ProcessingTimeTimerTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .keyBy(r=> true)
      .process(new KeyedProcessFunction[Boolean,Event,String] {
        override def processElement(i: Event, context: KeyedProcessFunction[Boolean, Event, String]#Context,
                                    collector: Collector[String]): Unit = {

          val currTs = context.timerService().currentProcessingTime()
          collector.collect("数据到达时间："+ new Timestamp(currTs))
          //注册定时器，10秒钟后执行定时器
          context.timerService().registerProcessingTimeTimer(currTs + 10 *1000L)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          out.collect("定时器触发，触发时间："+ new Timestamp(timestamp))
        }
      })
      .print
    env.execute()


  }
}
