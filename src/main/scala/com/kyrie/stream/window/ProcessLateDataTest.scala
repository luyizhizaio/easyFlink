package com.kyrie.stream.window

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration

//处理迟到数据-统计url 窗口内pv
object ProcessLateDataTest {

  case class UrlViewCount(url:String,count:Long,windowStart:Long,windowEnd:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("localhost",9090)
      .flatMap{data=>
        if(data !=null && data.trim != ""){

          val Array(user,url,timestamp)=data.split(",")
          Some(Event(user,url,timestamp.toLong))
        }else None
      }
      //1.设置水位线，延迟2秒
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[Event](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        })
      )

    //定义侧输出流标签
    val outputTag = OutputTag[Event]("late")

    val result = stream.keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.minutes(1))//允许窗口数据延迟一分钟
      .sideOutputLateData(outputTag) //定义测输出流，将迟到数据输出到侧输出流
      .aggregate(new UrlViewCountAgg ,new UrlViewCountResult)

    //result输出
    result.print("result")

    //侧输出流暑促
    result.getSideOutput(outputTag).print("late")

    stream.print("input")

    env.execute()
  }

  class UrlViewCountAgg extends AggregateFunction[Event,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc + 1L

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }

  class UrlViewCountResult extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def process(url: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(
        UrlViewCount(url,elements.iterator.next(),context.window.getStart,context.window.getEnd)
      )
    }
  }

}
