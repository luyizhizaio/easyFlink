package com.kyrie.stream.source

import org.apache.flink.streaming.api.scala._

/**
 * 集合中读取数据
 */



object Source2FromFile {
  case class Feedback(deviceId:String,idtype:String,media:String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("data/src.txt")

    stream.flatMap{line => line.split("\\s")}
      .map{word => WordWithCount(word,1)}
      .keyBy("word").sum("count").print().setParallelism(1)

    env.execute()



  }



}
