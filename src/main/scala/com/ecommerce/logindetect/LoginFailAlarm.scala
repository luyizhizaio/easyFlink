package com.ecommerce.logindetect

import com.ecommerce.LoginLog
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 一段时间内连续登录失败报警,两秒内连续两次登录失败
 */
case class FailAlarm(userId:Long,failStart:String,failEnd:String,msg:String)

object LoginFailAlarm {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("data/ecom/LoginLog.csv")
      .map{line =>
        val arr = line.split(",")
        LoginLog(arr(0).trim.toLong,arr(1).trim,arr(2).trim,arr(3).trim.toLong)
      }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(10)) {
      override def extractTimestamp(element: LoginLog): Long = element.timestamp
    })
      .keyBy(_.userId)
      .process(new LoginFailDetect(2))

    dataStream.print()
    env.execute()
  }

}

//检测登录次数，process function
class LoginFailDetect(maxFail: Int) extends KeyedProcessFunction[Long,LoginLog,FailAlarm]{

  lazy private val failListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("failListState",classOf[LoginLog]))

  override
  def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, FailAlarm]#Context, out: Collector[FailAlarm]): Unit ={

    if(value.state == "fail"){//失败
      val curList = failListState.get().iterator()

      if(curList.hasNext){//之前有失败
        val lastFail = curList.next()
        if(lastFail.timestamp > value.timestamp - 2){
          out.collect(FailAlarm(value.userId,lastFail.timestamp.toString,value.timestamp.toString,"fails in 2 times"))
          failListState.clear()
          failListState.add(value)
        } else {
          failListState.clear()
          failListState.add(value)
        }
      }else{ //第一次失败
        failListState.add(value)
      }

    }else{ //成功
      failListState.clear()
    }



  }
}

