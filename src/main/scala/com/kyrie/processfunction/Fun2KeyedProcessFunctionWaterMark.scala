package com.kyrie.processfunction

import com.kyrie.stream.watermark.Feedback2
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 实现：eventtime的keyedprocessfunction ,但遇到问题，watermark 没起到作用，currenteventtime 为integer_min
 */
object Fun1KeyedProcessFunctionWaterMark {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // nc -lk 9999
    val stream = env.socketTextStream("localhost",9999)

    val mapStream = stream.flatMap{line =>
      if(line.trim !=""){
        val Array(id ,ts,fre) =line.split(" ")
        Some(Feedback2(id,ts.toLong,fre.toInt))
      }else None
    }

    mapStream.print("keystream:")


   val wmStream = mapStream.assignTimestampsAndWatermarks(
     new BoundedOutOfOrdernessTimestampExtractor[Feedback2](Time.milliseconds(1000)) {
     //提取时间戳。单位是毫秒
     override
     def extractTimestamp(element: Feedback2): Long = {
       element.timestamp * 1000 //时间戳是秒 乘以1000转成毫秒
     }
   }).keyBy(_.id)

    wmStream.print("wmStream:")

    //执行process function
    val processStream = wmStream.process(new TempIncreaseAlertFunction2)

    processStream.print("process:")

    env.execute("key process function")

  }

}

/**
 * 实现功能：10秒内温度连续上升，发出报警
 */
class TempIncreaseAlertFunction2 extends KeyedProcessFunction[String,Feedback2,String]{

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
      println("delet timer:" +curTimerTimestamp)
      //删除定时器
      ctx.timerService().deleteEventTimeTimer(curTimerTimestamp)
      //清除定时时间戳
      currentTimerState.clear()
    }else if(fb.fre > lastTemp && curTimerTimestamp ==0){//温度上升

      //设置定时器执行时间，一秒之后执行
      val timerTs = ctx.timerService().currentWatermark() +10000
      println("set timerTs："+timerTs)

      //注册程序时间定时器
      ctx.timerService().registerEventTimeTimer(timerTs)

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


  }
}
