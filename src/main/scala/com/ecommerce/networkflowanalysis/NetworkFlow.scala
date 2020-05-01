package com.ecommerce.networkflowanalysis

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Created by tend on 2020/4/30.
 * 通过apachelog 统计热门页面
 * 如果延迟过大,watermark要求低延迟，所以后面还要对迟到数据做处理
 */

case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

case class UrlViewCount(url:String,windowEnd:Long,count:Long)


object NetworkFlow {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("data/ecom/apache2.log")
      .map{line =>
        val arr = line.split(" ")

        val format =new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val eventTime = format.parse(arr(3).trim).getTime

        ApacheLogEvent(arr(0).trim,arr(1).trim,eventTime,arr(5).trim,arr(6).trim)
      }
      .assignTimestampsAndWatermarks( //数据乱序，使用此
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60))//允许数据最大延迟，更新数据
      .aggregate(new FlowCountAgg(),new FlowWindowAgg())
      .keyBy(_.windowEnd)
      .process(new FlowTopN(5))

    dataStream.print()

    env.execute()



  }

}

/**
 * 窗口内聚合函数，窗口内聚合
 */
class FlowCountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def merge(acc: Long, acc1: Long): Long = acc + acc1

  override def getResult(acc: Long): Long = acc

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
}

/**
 * 窗口处理函数，接收AggregateFunction聚合后的结果
 */
class FlowWindowAgg() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override
  def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {

    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))

  }
}

//自定义processfunction 进行排序
class FlowTopN(topN: Int)extends KeyedProcessFunction[Long,UrlViewCount,String]{

  lazy val listState = getRuntimeContext.getListState(new ListStateDescriptor("url-top-state",classOf[UrlViewCount]))

  override
  def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

    listState.add(i) //存入state
    //注册定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override
  def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val listBuf = new ListBuffer[UrlViewCount]()

    val iter = listState.get().iterator()

    while(iter.hasNext){
      listBuf += iter.next()
    }


    val sortedBuf = listBuf.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    val result = new StringBuilder()

    result.append("时间：").append(timestamp - 1).append("\n")
    for(i <- sortedBuf.indices){

      val item = sortedBuf(i)
      result.append("No.").append(i +1).append(":")
        .append(" url = ").append(item.url)
        .append(" pv = ").append(item.count)
        .append("\n")
    }

    result.append("=========================\n")

    out.collect(result.toString())

  }
}
