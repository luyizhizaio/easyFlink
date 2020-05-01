package com.ecommerce.pvuv

import com.ecommerce.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * 使用bloomfilter统计uv
 */
object UVWithBloom {

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
      .map(ub => ("dummykey",ub.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //自定义trigger，每来一条数据都触发窗口执行
      .process(new BloomProcess())


    dataStream.print()

  }

}
//自定义窗口触发器，每来一条数据都会触发
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  override
  def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据都触发并且触发后删除窗口的状态。
    TriggerResult.FIRE_AND_PURGE
  }

  //到达processingtime，是继续还是触发执行窗口，这里是继续
  override
  def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override
  def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override
  def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

class BloomProcess extends ProcessWindowFunction[(String,Long),UVCount,String,TimeWindow]{

  lazy val redis = new Jedis("localhost",6397)

  val bloom = new Bloom(1 << 29)

  override
  def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVCount]): Unit = {



    val userId = elements.last._2.toString

    val offset = bloom.hash(userId,61)

    val windowEnd = context.window.getEnd.toString
    var count = 0L
    //uv 数量
    if(redis.hget("count",windowEnd) !=null){
      count = redis.hget("count",windowEnd) .toLong
    }

    val bool = redis.getbit(windowEnd,offset)
    if(!bool){ //ID不存在
      // 将offset 存入redis bitmap
      redis.setbit(windowEnd,offset,true)

      redis.hset("count",windowEnd,(count + 1).toString)
      out.collect(UVCount(windowEnd.toLong,count +1))
    }else{
      out.collect(UVCount(windowEnd.toLong,count))
    }
  }
}



//用于生成offset
class Bloom(size:Long) extends Serializable{
  //位图总大小
  private val cap = if (size > 0 )size else 1 << 27

  def hash (value :String,seed:Int):Long={
    var result =0L
    for (i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    result & (cap -1)

  }


}
