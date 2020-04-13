package com.kyrie.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object RichFunction {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("data/src2.txt")

    //使用富函数
    stream.flatMap(new MyFlatMap()).print("rich:")


    env.execute()
  }

}




class MyFlatMap extends RichFlatMapFunction[String,(Int,Int)]{

  var  subTaskIndex = 0

  /**
   * 只执行一次
   * @param parameters
   */
  override
  def open(parameters: Configuration): Unit = {

    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask

  }

  override def close(): Unit ={
    super.close()
  }

  /**
   *
   * @param in 输入
   * @param out 输出
   */
  override def flatMap(in: String, out: Collector[(Int, Int)]): Unit = {

    val Array(word,num) = in.split(" ")

    if(num.toInt % 2 == subTaskIndex){
      out.collect(subTaskIndex -> num.toInt)
    }

  }
}
