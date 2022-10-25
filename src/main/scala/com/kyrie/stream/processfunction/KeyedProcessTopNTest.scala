package com.kyrie.stream.processfunction

import com.kyrie.stream.source.{ClickSource, Event}
import com.kyrie.stream.window.ProcessLateDataTest.UrlViewCount
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object KeyedProcessTopNTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val eventStream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    eventStream.print("eventStream:")

    val urlCountStream = eventStream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .aggregate(new UrlViewCountAgg,new UrlViewCountResult)

    urlCountStream.print("urlCountStream:")

    val result = urlCountStream.keyBy(_.windowEnd) //根据窗口分组
      .process(new TopN(2))

    result.print()

    env.execute()
  }

  class UrlViewCountAgg extends AggregateFunction[Event,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc +1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }

  class UrlViewCountResult extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def process(url: String, context: Context,
                         elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(
        UrlViewCount(url,elements.iterator.next(),context.window.getStart,context.window.getEnd)
      )

    }
  }

  class TopN(n: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

    var urlViewCountListState:ListState[UrlViewCount] = _

    override def open(parameters: Configuration): Unit = {
      //定义状态
      urlViewCountListState = this.getRuntimeContext.getListState(
        new ListStateDescriptor[UrlViewCount]("list-state",classOf[UrlViewCount])
      )
    }

    //处理每一个元素
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                                collector: Collector[String]): Unit = {
      //每个元素添加到状态中
      urlViewCountListState.add(i)

      //注册定时任务，窗口结束后执行(1毫秒之后执行)
      context.timerService.registerEventTimeTimer(i.windowEnd +1)
    }

    /**
     * 定时执行
     * @param timestamp 定时触发时间
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      import scala.collection.JavaConversions._
      val urlCountList = urlViewCountListState.get().toList

      //清空状态变量
      urlViewCountListState.clear()

      urlCountList.sortBy(- _.count)

      val result = new StringBuilder

      result.append("==============\n")
      for(i <- 0 until n){
        val urlViewCount = urlCountList(i)

        result.append("浏览量No."+(i+1)+" ")
          .append("url:"+ urlViewCount.url +" ")
          .append("浏览量："+ urlViewCount.count +" ")
          .append("窗口结束时间："+ new Timestamp(urlViewCount.windowEnd) +"\n")
      }
      result.append("================\n")
      out.collect(result.toString())
    }
  }
}
