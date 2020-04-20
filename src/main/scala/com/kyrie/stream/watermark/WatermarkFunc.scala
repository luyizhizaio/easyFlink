package com.kyrie.stream.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 处理顺序的数据
 * 如果我们事先得知数据流的时间戳是单调递增的，也就是说没有乱序，那我们可以使用 assignAscendingTimestamps，
 * 这个方法会直接使用数据的时间戳生成 watermark。
 */
case class Feedback2(id:String,timestamp:Long,fre:Int)
object WatermarkFunc {

  """
    |水位线用于乱序数据
    | Watermark 是一种衡量 Event Time 进展的机制。
    | Watermark 是用于处理乱序事件的，而正确的处理乱序事件，通常用
    |Watermark 机制结合 window 来实现。
    | 数据流中的 Watermark 用于表示 timestamp 小于 Watermark 的数据，都已经
    |到达了，因此，window 的执行也是由 Watermark 触发的。
    | Watermark 可以理解成一个延迟触发机制，我们可以设置 Watermark 的延时
    |时长 t，每次系统会校验已经到达的数据中最大的 maxEventTime，然后认定 eventTime
    |小于 maxEventTime - t 的所有数据都已经到达，如果有窗口的停止时间等于
    |maxEventTime – t，那么这个窗口被触发执行。
    |
    |Watermark 就是触发前一窗口的“关窗时间”，一旦触发关门那么以当前时刻为准在窗口范围内的所有所有数据都会收入窗中。
    |只要没有达到watermark那么不管现实中的时间推进了多久都不会触发关窗。
    |""".stripMargin

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置为事件事件，原数据要包含timestamp字段，才能生效
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置检查水位线时间 ，默认200毫米
    env.getConfig.setAutoWatermarkInterval(200)

    env.setParallelism(1)

    val stream =env.socketTextStream("localhost",9999)

    val textWithTsDstream = stream.map{text =>
      val arr:Array[String] = text.split(" ")
      Feedback2(arr(0),arr(1).toLong,1)
    }

    //指定timestamp字段;秒转成毫秒
    val textWithEventTimeStream = textWithTsDstream.assignAscendingTimestamps(e => e.timestamp *1000)



    val keyStream = textWithEventTimeStream
      .keyBy(_.id)
    keyStream.print("keytext:")
    val windowStream = keyStream
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))


    windowStream.reduce{(a,b) => (Feedback2(a.id,a.timestamp + b.timestamp, a.fre + b.fre))}.print("window:")

    env.execute()



  }

}
