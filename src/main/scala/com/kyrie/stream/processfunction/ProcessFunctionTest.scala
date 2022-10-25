package com.kyrie.stream.processfunction

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.streaming.api.functions._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      //处理函数
      .process(new ProcessFunction[Event,String]{
        override def processElement(i: Event, context: ProcessFunction[Event, String]#Context,
                                    collector: Collector[String]): Unit = {
          if(i.user =="lbj"){
            collector.collect("lbj" + "_"+"lbj")
          }else{
            collector.collect(i.user)
          }

          val wm = context.timerService().currentWatermark()
          println(s"wm:${wm}")
        }
      }
      )
      .print("xx")
    env.execute()
  }

}
