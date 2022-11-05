package com.kyrie2

import org.apache.flink.streaming.api.scala._

/**
 *
 */
object BoundedStreamWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(1)

    val ds = env.readTextFile("data/src.txt")

    val result = ds.flatMap(_.split(" "))
      .map(w => w -> 1)
      .keyBy(_._1)
      .sum(1)

    result.print("xx")

    env.execute("bounded stream")
  }
}
