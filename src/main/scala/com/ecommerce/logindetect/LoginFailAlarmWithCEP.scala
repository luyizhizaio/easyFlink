package com.ecommerce.logindetect

import java.util

import com.ecommerce.LoginLog
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用cep实现：一段时间内连续登录失败报警,两秒内连续两次登录失败
 */
object LoginFailAlarmWithCEP {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("data/ecom/LoginLog.csv")
      .map { line =>
        val arr = line.split(",")
        LoginLog(arr(0).trim.toLong, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(10)) {
      override def extractTimestamp(element: LoginLog): Long = element.timestamp
    })
      .keyBy(_.userId)

    //2.根据需求定义pattern :两分钟内连续失败两次；begin，next 分别代表两个事件的名称，where指定这次事件的条件。
    val failLoginPattern = Pattern.begin[LoginLog]("begin").where(_.state == "fail")
        .next("next").where(_.state == "fail")
      .within(Time.seconds(2))

    //得到pattern stream
    val patternStream = CEP.pattern(dataStream,failLoginPattern)

    //从pattern stream 上应用select function检出匹配时间序列
    val loginFailDataStream = patternStream.select(new LogingFailMatch())

    loginFailDataStream.print()

    env.execute()

  }

}
//select function
class LogingFailMatch() extends PatternSelectFunction[LoginLog,FailAlarm]{
  override def select(map: util.Map[String, util.List[LoginLog]]): FailAlarm = {
    //把所有检测到的时间序列放到参数map里了

      //从map中根据名称取出事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    FailAlarm(firstFail.userId,firstFail.timestamp.toString,lastFail.timestamp.toString,"fail")

  }
}
