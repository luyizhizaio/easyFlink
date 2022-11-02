package com.kyrie.stream.transform

import com.kyrie.stream.source.ClickSource
import org.apache.flink.streaming.api.scala._

object ShuffleTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    stream.shuffle.print("shuffle:").setParallelism(5)

    env.execute()
  }


}