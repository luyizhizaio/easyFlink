package com.kyrie.stream.source

import org.apache.flink.streaming.api.scala._

/**
 * 集合中读取数据
 */

case class Feedback(deviceId:String,idtype:String,media:String)

object Source1FromCollection {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromCollection(Seq(
      Feedback("q1w2e3","mac","sina"),
      Feedback("q1w2e3","mac","sina"),
      Feedback("dcdcw2e3","idfa","tx"),
      Feedback("q1w2e3","mac","mt")
    ))

    stream1.keyBy(_.media).print("1").setParallelism(3) //设置并行度

    env.execute()



  }



}
