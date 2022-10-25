package com.kyrie.stream.state

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 定时统计pv
 * 注册了一个定时器，用来隔一段时间发送 pv 的统计结果，这样对下游算子的压力不至于太大
 */
object PeriodicPvTestValueState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.print("input")

    stream.keyBy(_.user)
      .process(new PeriodicPvResult)//自定义处理函数
      .print("result")


    env.execute()
  }

  class PeriodicPvResult extends  KeyedProcessFunction[String,Event,String] {

    //保持pv状态
    lazy val countState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count",classOf[Long]))

    //保存定时器时间戳
    lazy val timerTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer-ts",classOf[Long])
    )


    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context,
                                collector: Collector[String]): Unit = {
      //获取值
      val count = countState.value()
      //更新
      countState.update(count +1)

      if(timerTsState.value() == 0){
        //注册定时器
        context.timerService().registerEventTimeTimer(i.timestamp + 10* 1000L)
        timerTsState.update(i.timestamp + 10*1000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      //往下游发送数据
      out.collect("用户 "+ ctx.getCurrentKey + " 的pv是" + countState.value())
      timerTsState.clear()
    }
  }

}
