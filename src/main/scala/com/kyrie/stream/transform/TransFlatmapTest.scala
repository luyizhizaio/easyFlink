package com.kyrie.stream.transform

import com.kyrie.stream.source.Event
import org.apache.flink.streaming.api.scala._

object TransFlatmapTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(Event("ad", "./home", 1000L), Event("lbj", "./cart,./home", 2000L))

    stream.flatMap{e => e.url.split(",").map{url=>
      Event(e.user,url,e.timestamp)
    }}.print()

    env.execute()

  }

}
