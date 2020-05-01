package com.ecommerce.pvuv

import com.ecommerce.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 根据埋点日志统计pv
 */
object PageView {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("data/ecom/UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserBehavior(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim.toInt,arr(3).trim,arr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //秒转成毫秒
      .filter(_.behavior == "pv")
      .map(_=>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count :")

    env.execute()


  }


}
