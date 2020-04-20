package com.kyrie.stream.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 集合中读取数据
 */



object Source1FromCollection {
  case class Feedback(deviceId:String,idtype:String,media:String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromCollection(Seq(
      Feedback("q1w2e3","mac","sina"),
      Feedback("q1w2e3","mac","sina"),
      Feedback("dcdcw2e3","idfa","tx"),
      Feedback("q1w2e3","mac","mt")
    ))

    stream1.keyBy("media").print().setParallelism(1)

    env.execute()



  }



}
