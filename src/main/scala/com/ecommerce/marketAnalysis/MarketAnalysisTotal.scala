package com.ecommerce.marketAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 整体统计
 */
object MarketAnalysisTotal {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new MarketLogSourceFunction())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior !="UNINSTALL")
      .map(data => {("dummy",1L)})
      .keyBy(_._1)
      .timeWindow(Time.hours(2),Time.seconds(10))
      .aggregate(new MarketCountAgg,new MarketWindowAgg)

    dataStream.print()

    env.execute()

  }

}

class MarketCountAgg extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketWindowAgg extends WindowFunction[Long,AnalysisResult,String,TimeWindow]{
  override
  def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AnalysisResult]): Unit = {

    val windowStart  = new Timestamp(window.getStart).toString
    val windowEnd  = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()

    out.collect(AnalysisResult(windowStart,windowEnd,"market","total",count))
  }
}
