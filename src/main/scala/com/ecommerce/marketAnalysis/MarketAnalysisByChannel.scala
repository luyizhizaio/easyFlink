package com.ecommerce.marketAnalysis

import java.sql.Timestamp
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 市场分析统计：统计各渠道的情况
 */

//输入数据
case class MarketLog(userId:String,channel:String,behavior: String,timestamp:Long)

//输出数据
case class AnalysisResult(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)

object MarketAnalysis {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new MarketLogSourceFunction())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior !="UNINSTALL")
      .map(data => {((data.channel,data.behavior),1L)})
      .keyBy(_._1)
      .timeWindow(Time.hours(2),Time.seconds(10))
      .process(new AnalysisResultProcess)

    dataStream.print()

    env.execute()

  }

}

class MarketLogSourceFunction() extends RichSourceFunction[MarketLog]{

  private  var running = true

  private var rand = new Random()

  private val max = Long.MaxValue


  private val channelTypes = Seq("appstore","weibo","wechat","xiaomistore")

  private val behaviorTypes = Seq("INSTALL","CLICK","DOWNLOAD","UNINSTALL")

  override def run(ctx: SourceFunction.SourceContext[MarketLog]): Unit = {

    var count = 0

    while(running & count < max ){

      val userId = UUID.randomUUID().toString
      val channel = channelTypes(rand.nextInt(channelTypes.size))
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val timestamp = new Date().getTime

      TimeUnit.MILLISECONDS.sleep(10) //sleep

      ctx.collect(MarketLog(userId,channel,behavior,timestamp))
    }


  }

  override def cancel(): Unit = running = false
}

class AnalysisResultProcess extends ProcessWindowFunction[((String,String),Long),AnalysisResult,(String,String),TimeWindow]{
  override
  def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[AnalysisResult]): Unit = {

    val channel = key._1
    val behavior = key._2

    val windowStart  = new Timestamp(context.window.getStart).toString
    val windowEnd  = new Timestamp(context.window.getEnd).toString
    val count = elements.size

   out.collect(AnalysisResult(windowStart,windowEnd,channel,behavior,count))

  }
}
