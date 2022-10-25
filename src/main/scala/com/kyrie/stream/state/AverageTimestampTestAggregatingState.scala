package com.kyrie.stream.state

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * 对用户点击事件流每 5 个数据统计一次平均时间戳
 */
object AverageTimestampTestAggregatingState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new AvgTsResult)
      .print()

    env.execute()

  }
  class AvgTsResult extends RichFlatMapFunction[Event,String]{


    lazy val avgTsAggState = getRuntimeContext.getAggregatingState(
      new AggregatingStateDescriptor[Event,(Long,Long),Long](
        "avg-ts",
        new AggregateFunction[Event,(Long,Long),Long] {
          override def createAccumulator(): (Long, Long) = (0L,0L)

          override def add(in: Event, acc: (Long, Long)): (Long, Long) = (acc._1 + in.timestamp,acc._2 +1)

          override def getResult(acc: (Long, Long)): Long = acc._1 /acc._2

          override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = ???
        }
        ,classOf[(Long,Long)]
      )
    )

    //用户点击频次
    lazy val countState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count",classOf[Long])
    )

    override def flatMap(in: Event, collector: Collector[String]): Unit = {

      val count = countState.value()
      countState.update(count +1)

      avgTsAggState.add(in)

      if(count == 5){
        collector.collect(in.user + " 平 均 时 间 戳 ： " + new
            Timestamp(avgTsAggState.get()))
        countState.clear()
      }

    }
  }
}
