package com.kyrie2

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object UdfFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds = env.fromElements(
      Event("lbj", "./home", 1000L),
      Event("ad", "./cart", 2000L)
    )

    ds.filter(new FlinkFilter).print()


    env.execute()
  }

  class FlinkFilter extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = {
      t.url.contains("home")
    }
  }
}
