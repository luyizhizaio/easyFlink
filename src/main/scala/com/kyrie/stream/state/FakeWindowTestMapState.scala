package com.kyrie.stream.state

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 模拟实现滚动窗口-统计每 10s 滚动窗口内，每个 url 的 pv
 */
object FakeWindowTestMapState{
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .process(new FakeWindowResult(10 *1000L)) //自定义处理函数
      .print()

    env.execute()
  }

  class FakeWindowResult(windowSize:Long) extends  KeyedProcessFunction[String,Event,String]{

    lazy val windowPvMapState = getRuntimeContext.getMapState(
      new MapStateDescriptor[Long,Long]("window-pv",classOf[Long],classOf[Long])
    )


    override def processElement(event: Event, context: KeyedProcessFunction[String, Event, String]#Context,
                                collector: Collector[String]): Unit = {

      val windowStart = event.timestamp/windowSize * windowSize
      val windowEnd = windowStart + windowSize

      //注册定时器，触发窗口
      context.timerService().registerEventTimeTimer(windowEnd -1)

      if(windowPvMapState.contains(windowStart)){
        val pv = windowPvMapState.get(windowStart)
        windowPvMapState.put(windowStart,pv +1L)
      }else{
        windowPvMapState.put(windowStart,1L)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext,
                         out: Collector[String]): Unit ={

      val windowEnd = timestamp +1L
      val windowStart = windowEnd - windowSize

      out.collect(s"url:${ctx.getCurrentKey},pv:${windowPvMapState.get(windowStart)}; window:${windowStart}~${windowEnd}")

    windowPvMapState.remove(windowStart)

    }
  }

}
