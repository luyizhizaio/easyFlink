package com.kyrie.stream.source

import org.apache.flink.streaming.api.scala._

/**
 * 集合中读取数据
 */



object Source2FromFile {
  case class Feedback(deviceId:String,idtype:String,media:String)
  case class WordWithCount(word:String,count:Int)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //设置全局并行度

    val stream = env.readTextFile("data/src.txt")

    stream.flatMap{line => line.split("\\s")}
      .map{word => WordWithCount(word,1)}
      .keyBy(_.word).sum("count").print("1")

    env.execute()
  }
}
