package com.kyrie.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import scala.collection.mutable


object Watermark2TumblingEventTimeWindows {

  """
    |水位线+windows 用于处理乱序数据
   """

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
      (arr(0),arr(1).toLong,1)
    }
    //处理乱序，设置watermark,最大等待时间为1000毫秒
    val textWithEventTimeStream = textWithTsDstream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
        //提取时间戳。单位是毫秒
        override
        def extractTimestamp(element: (String, Long, Int)): Long = {
          element._2 * 1000 //时间戳是秒 乘以1000转成秒
        }
      })

    val textKeyStream = textWithEventTimeStream.keyBy(0)

    textKeyStream.print("textkey:")

    val windowStream =textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))

    val reduceStream = windowStream.reduce((a,b) => (a._1, a._2 +b._2,a._3 +b._3))

    reduceStream.print("window::").setParallelism(1)

   /* val groupStream = windowStream.fold(new mutable.HashSet[Long]()){
      case (set,(key,ts,count)) =>set += ts
    }

    groupStream.print("window：：").setParallelism(1)*/

    env.execute()



  }

}
