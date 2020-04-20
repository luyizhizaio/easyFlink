package com.kyrie.processfunction

import com.kyrie.stream.watermark.Feedback2
import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object Fun1KeyedProcessFunction {

  def main(args: Array[String]): Unit = {

    """
      |process function
      |转换算子是无法访问事件的时间戳信息和水位线信息的。
      |基于此，DataStream API 提供了一系列的 Low-Level 转换算子。可以访问时间戳、watermark 以及注册定时事件。
      |
      |KeyedProcessFunction 用来操作 KeyedStream；继承富函数的类 。
      |KeyedProcessFunction 会处理流的每一个元素，输出为 0 个、1 个或者多个元素。
      |""".stripMargin

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // nc -lk 9999
    val stream = env.socketTextStream("localhost",9999)

    val keyStream = stream.map{line =>
      val Array(id ,ts,fre) =line.split(" ")
      Feedback2(id,ts.toLong,fre.toInt)
    }.keyBy(_.id)

    keyStream.print("keystream:")

    //执行process function
    val processStream = keyStream.process(new TempIncreaseAlertFunction)

    processStream.print("process:")

    env.execute("key process function")

  }

}

/**
 * 实现功能：10秒内温度连续上升，发出报警
 */
class TempIncreaseAlertFunction extends KeyedProcessFunction[String,Feedback2,String]{

  /**
   * 使用状态存储 上一次的数据
   */
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp",Types.of[Double]))

  /**
   * 保存注册定时任务的时间戳
   */

  lazy val currentTimerState:ValueState[Long] =getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer",Types.of[Long]))


  /**
   * 每个元素都会执行这个函数
   * @param fb
   * @param ctx
   * @param out
   */
  override
  def processElement(fb: Feedback2, ctx: KeyedProcessFunction[String, Feedback2, String]#Context, out: Collector[String]): Unit = {

    //
    val curTimerTimestamp = currentTimerState.value()

    val lastTemp = lastTempState.value()
    //保存当前维度
    lastTempState.update(fb.fre)

    if(lastTemp == 0.0 || fb.fre < lastTemp){//温度下降

      //删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      //清除定时时间戳
      currentTimerState.clear()
    }else if(fb.fre > lastTemp && curTimerTimestamp ==0){//温度上升

      //设置定时器执行时间，一秒之后执行
      val timerTs = ctx.timerService().currentProcessingTime() +10000
      println("set timerTs："+timerTs)
      //注册程序时间定时器
      ctx.timerService().registerProcessingTimeTimer(timerTs)

      //保存定时器时间戳
      currentTimerState.update(timerTs)


    }

  }

  /**
   * 定时触发函数：当之前注册的定时器触发时调用。参数 timestamp 为定时器所设定的触发的时间戳
   * @param timestamp The timestamp of the firing timer.
   * @param ctx
   * @param out
   */
  override
  def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Feedback2, String]#OnTimerContext, out: Collector[String]): Unit = {

    // timestamp是触发时的时间戳
    println("ontimer timestamp:" + timestamp)

    out.collect("传感器ID为"+ctx.getCurrentKey+"10秒内温度连续上升")
    currentTimerState.clear()

    /**日志：
    keystream::3> Feedback2(id1,1586962181,1)
keystream::3> Feedback2(id1,1586962181,10)
set timerTs：1587218352141
ontimer timestamp:1587218352141
process::3> 传感器ID为id110秒内温度连续上升
     */


  }
}
