package com.kyrie.stream.source

import com.kyrie.stream.MyFeedbackSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 自定义source
 */
object Source4Custom {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //自定义source ，只需要继承SourceFunction即可
    val stream1 = env.addSource(new MyFeedbackSource())

    stream1.keyBy("media").print().setParallelism(1)


    env.execute("pangzi")


  }

}
