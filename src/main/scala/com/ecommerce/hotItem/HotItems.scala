package com.ecommerce.hotItem

import java.sql.Timestamp

import com.ecommerce.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 根据pv访问量统计热门商品
 */


//count 数量
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val datastream = env.readTextFile("data/ecom/UserBehavior.csv")
      .map(line=>{
      val arr = line.split(",")
      UserBehavior(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim.toInt,arr(3).trim,arr(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000)

    val processStream = datastream.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new CountAgg(),new WindowAgg())  //窗口聚合,有状态的算子；两个参数：预聚合函数，窗口聚合函数
      .keyBy(_.windowEnd) //按照窗口分组
      .process(new TopNItems(5)) //状态编程：实现排序 取出topN

    processStream.print()

    env.execute("hot item")

  }

}
//自定义预聚合函数 统计总记录数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
//自定义预聚合函数； 计算平均数 ，中间结果是总数量，总记录数
class AverageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double]{
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.category ,accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 /accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) =(a._1 + b._1,a._2 + b._2)
}
//自定义窗口聚合函数 ,其输入就是CountAgg的输出
class WindowAgg() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override
  def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}
//保存窗口内所有元素，进行排序
class TopNItems(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{


  private var itemsState:ListState[ItemViewCount] =_


  override def open(parameters: Configuration): Unit = {
    itemsState= getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("items-state",classOf[ItemViewCount]))
  }

  override
  def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

    //item 放入state
    itemsState.add(value)

    //定义timer ;Registers a timer to be fired when the event time watermark passes the given time.
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override
  def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    import scala.collection.JavaConversions._
    val allItems = new ListBuffer[ItemViewCount]
    for(item <- itemsState.get()){
      allItems += item
    }

    //排序,拿出topN
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态
    itemsState.clear()

    //格式化结果
    val result = new StringBuilder()

    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedItems.indices){
      val item = sortedItems(i)
      result.append("No").append(i +1).append(":")
        .append(" 商品ID=").append(item.itemId)
        .append("pv=").append(item.count)
        .append("\n")
    }

    result.append("=========================\n")

    out.collect(result.toString())
  }
}





