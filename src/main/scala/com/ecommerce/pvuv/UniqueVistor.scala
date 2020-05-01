package com.ecommerce.pvuv

import com.ecommerce.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 统计uv
 */

case class UVCount(windowEnd:Long,count:Long)
object UniqueVistor {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.readTextFile("data/ecom/UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserBehavior(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim.toInt,arr(3).trim,arr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //秒转成毫秒
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))//没有keyed的开窗
      .apply(new UVCountByWindow()) //针对每个窗口的操作

    dataStream.print()

    env.execute()

  }

}
//窗口聚合函数
class UVCountByWindow() extends AllWindowFunction[UserBehavior,UVCount,TimeWindow]{
  override
  def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {

    var items = Set[Long]()


    for(u <- input){
      items += u.userId
    }

    out.collect(UVCount(window.getEnd,items.size))

  }
}
