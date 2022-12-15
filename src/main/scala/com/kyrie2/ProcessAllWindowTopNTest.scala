package com.kyrie2

import com.kyrie.stream.source.ClickSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable

object ProcessAllWindowTopNTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource( new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .map(_.url)
      .windowAll(SlidingEventTimeWindows
        .of(Time.seconds(10),Time.seconds(5)))
      .process(new ProcessAllWindowFunction[String,String,TimeWindow] {

        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          val urlCountMap = mutable.Map.empty[String,Long]
          //
          elements.foreach(r=>
            urlCountMap.get(r) match{
              case Some(count) => urlCountMap.put(r,count +1)
              case None => urlCountMap.put(r,1L)
            }
          )

          val mapList = urlCountMap.toList.sortBy(-_._2)

          val result = new StringBuilder

          result.append("======\n")
          for(i <- 0 to 1){
            val tmp = mapList(i)
            result.append(s"浏览量NO.${i+1} url :${tmp._1},cnt:${tmp._2}; window end:${new Timestamp(context.window.getEnd)}\n")
          }

          result.append("========\n")

          out.collect(result.toString())
        }

      }).print()


    env.execute()
  }

}
