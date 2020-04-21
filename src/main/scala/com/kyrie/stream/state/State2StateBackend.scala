package com.kyrie.stream.state

import com.kyrie.stream.watermark.Feedback2
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import sun.plugin2.jvm.CircularByteBuffer.Streamer

/**
 * 状态后端
 */
object State2StateBackend {

  def main(args: Array[String]): Unit = {

    """
      |状态后端两个功能：负责本地状态管理，将检查点状态写入远程存储。
      |""".stripMargin


    val  env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置状态后端
    env.setStateBackend(new MemoryStateBackend())


    val stream = env.socketTextStream("localhost",9999)


    val keyStream = stream.map{line =>
      val Array(id,timestamp,fre) = line.split(" ")
      Feedback2(id,timestamp.toLong, fre.toInt)
    }.keyBy(_.id)

    keyStream.print("key:")

    //1.定义flatMap类，并执行
    //val flatStream = keyStream.flatMap(new TemperatureAlertFunction2(10))

    //2.使用flatmapstate方式实现
    val flatStream = keyStream.flatMapWithState[(String,Int,Int),Int]{
      case(fb:Feedback2,None) =>
        (List.empty,Some(fb.fre))
      case (fb:Feedback2,lastTemp:Some[Int]) =>
        val tempdiff = (fb.fre - lastTemp.get).abs
        if (tempdiff > 10){
          (List((fb.id,lastTemp.get,fb.fre)),Some(fb.fre))
        }else{
          (List.empty,Some(fb.fre))
        }
    }

    flatStream.print("flat:")

    env.execute()

  }

}

class TemperatureAlertFunction2(threshold:Int) extends RichFlatMapFunction[Feedback2,(String,Int,Int)]{

  private var lastTempState:ValueState[Int] =_

  override def open(parameters: Configuration): Unit = {

    val descriptor = new ValueStateDescriptor[Int]("lastTemp",classOf[Int])

    lastTempState = getRuntimeContext.getState[Int](descriptor)


  }

  override def flatMap(value: Feedback2, out: Collector[(String, Int, Int)]): Unit = {

    val lastTemp = lastTempState.value()

    val diff = (value.fre - lastTemp).abs

    if(diff > threshold){
      out.collect((value.id, lastTemp,value.fre))
    }else{
      this.lastTempState.update(value.fre)
    }
  }
}




