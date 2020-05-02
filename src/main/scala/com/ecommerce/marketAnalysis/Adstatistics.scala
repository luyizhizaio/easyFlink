package com.ecommerce.marketAnalysis

import java.sql.Timestamp

import com.ecommerce.AdClickLog
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 广告统计:统计每个省份的点击情况;进行黑名过滤，黑名单从侧输出流输出
 */

case class AdstatResult(windowEnd:String,province:String,count:Long)

case class BlacklistAlarm(userId:Long,adId:Long,msg:String)

object Adstatistics {
  //侧输出流标记
  val alarmSlideOutTag = new OutputTag[BlacklistAlarm]("blacklistAlarm")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("data/ecom/AdClickLog.csv")
      .map(line =>{
        val arr  = line.split(",")
        AdClickLog(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim,arr(3).trim,arr(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp)

    //过滤点击量生成黑名单
    val filteredStream = dataStream.keyBy(data=>(data.userId,data.adId))
      .process(new FilterBlacklistProcessFunction(100))


    //分组，开窗统计省份点击量
    val aggStream = filteredStream.keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .aggregate(new AdCountAgg,new AdWindowAgg)

    aggStream.print()
    //获取侧输出流
    val alarmStream = filteredStream.getSideOutput(alarmSlideOutTag)

    alarmStream.print()

    env.execute()


  }

  class FilterBlacklistProcessFunction(maxClick: Int) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{

    //记录用户对广告的点击数
    lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("click-count",classOf[Long]))

    //记录是否发送报警
    lazy val isAlarmState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("alarm",classOf[Boolean]))

    //保存定时任务时间戳
    lazy val timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))

    override
    def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

      val curCount = countState.value()

      if(curCount ==0L){//第一次点击,注册定时任务：第二天凌晨清空状态
        val timerTs = (ctx.timerService().currentProcessingTime()/(1000*60*60*24)+1) * (1000*60*60*24)
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        timerTsState.update(timerTs)
      }


      if(curCount >= maxClick){//大于限制，就不输出了

        val isAlarm = isAlarmState.value()
        if(!isAlarm){
          isAlarmState.update(true)
          //侧输出流，报警
          ctx.output(alarmSlideOutTag,BlacklistAlarm(value.userId,value.adId,s"exceed ${maxClick} limit"))
        }
        return
      }
      val newCount = curCount +1
      countState.update(newCount)
      out.collect(value)


    }

    override
    def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

      if(timestamp == timerTsState.value()){
        //清除前一天的状态
        countState.clear()
        isAlarmState.clear()
        timerTsState.clear()
      }

    }
  }

}

class AdCountAgg extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdWindowAgg extends WindowFunction[Long,AdstatResult,String,TimeWindow]{
  override
  def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdstatResult]): Unit = {

    val windowEnd = new Timestamp(window.getEnd).toString
    out.collect(AdstatResult(windowEnd,key,input.iterator.next()))
  }
}
