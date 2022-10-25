package com.kyrie.stream.window

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 聚合函数 - 计算平均pv
 */
object AggregateFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_=> "key")//数据发到同一个分区
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
      .aggregate(new AvgPv)
      .print()

    env.execute()
  }

  //范型：输入，累加器，输出
  class AvgPv extends AggregateFunction[Event,(Set[String],Long),Double]{
    override def createAccumulator(): (Set[String], Long) = (Set[String](),0L)

    override def add(in: Event, acc: (Set[String], Long)): (Set[String], Long) = {
      (acc._1 +in.user , acc._2 +1L)
    }

    override def getResult(acc: (Set[String], Long)): Double = acc._2.toDouble /acc._1.size

    override def merge(acc: (Set[String], Long), acc1: (Set[String], Long)): (Set[String], Long) = ???
  }

}
