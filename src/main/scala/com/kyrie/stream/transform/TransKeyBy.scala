package com.kyrie.stream.transform

import com.kyrie.stream.source.Event
import org.apache.flink.streaming.api.scala._

object TransKeyBy {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromElements(Event("ad", "./home", 1000L), Event("lbj", "./cart", 2000L))

    val keyedStream = stream.keyBy(_.user)

    keyedStream.print()

    env.execute()

  }

}
