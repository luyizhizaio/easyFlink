package com.kyrie.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Created by tend on 2019/1/30.
 */
case class WordWithCount(word: String, count: Long)

object SocketWindowWordCount {

  def main(args: Array[String]) {


    val port =7777

    //1.获取执行环境
    // getExecutionEnvironment 会根据查询运行的方式决定返回什么样的运行环境，

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.获取输入数据（socket连接的数据）
    val text = env.socketTextStream("192.168.0.104",port,'\n')

    //3.transformation 数据
    val windowCounts = text
      .flatMap{w => w.split("\\s")}
      .map{w =>WordWithCount(w,1)}
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")


    //4.指定输出计算结果：单线程打印结果
    windowCounts.print().setParallelism(1)

    //5.触发执行任务
    env.execute("Socket Window WordCount")
  }

}


