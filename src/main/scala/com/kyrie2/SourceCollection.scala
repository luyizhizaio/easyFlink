package com.kyrie2

import org.apache.flink.streaming.api.scala._

object SourceCollection {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val list =List("a","b","c")
//
//    env.fromCollection(list).print()


    env.fromElements("a","b","c").print()

    env.execute("second")
  }

}
