package com.kyrie2

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val ds = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L)
    )

    ds.map(new RichMapFunction[Event,String] {


      override def open(parameters: Configuration): Unit = {
        println("index:"+getRuntimeContext.getIndexOfThisSubtask + "start")
      }

      override def close(): Unit ={
        println("index" +getRuntimeContext.getIndexOfThisSubtask +"end")
      }

      override def map(in: Event): String = {
        in.url +"&&"+ in.user
      }
    })
      .print()

    env.execute()

  }
}
