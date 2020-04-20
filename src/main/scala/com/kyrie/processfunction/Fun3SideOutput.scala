package com.kyrie.processfunction

import com.kyrie.stream.watermark.Feedback2
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 侧输出流demo
 */
object Fun3SideOutput {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost",9999)

    val mapStream = stream.flatMap{line =>
      if(line.trim !=""){
        val Array(id ,ts,fre) =line.split(" ")
        Some(Feedback2(id,ts.toLong,fre.toInt))
      }else None
    }

    mapStream.print("mapStream:")

    val processStream = mapStream.process(new ClickAlert)

    processStream.print("main out:")

    //根据tag获取侧输出流
    val sideOutputStream = processStream.getSideOutput(new OutputTag[String]("clickAlert"))
    sideOutputStream.print("side out:")

    env.execute()

  }


}

/**
 * 报警信息输入到侧输出流，
 *
 */
class ClickAlert extends ProcessFunction[Feedback2,Feedback2]{ // 主输出流类型

  lazy val outputTag:OutputTag[String] = new OutputTag[String]("clickAlert")
  override
  def processElement(value: Feedback2, ctx: ProcessFunction[Feedback2, Feedback2]#Context, out: Collector[Feedback2]): Unit = {

    val fre = value.fre

    if(fre >10){//异常，
      // 侧输出流,参数：标记，侧流数据
      ctx.output(outputTag,"id="+value.id+"有异常行为")
    }else{ //主输出流
      out.collect(value)
    }

  }


}


