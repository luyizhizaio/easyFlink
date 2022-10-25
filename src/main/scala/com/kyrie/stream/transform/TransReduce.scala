package com.kyrie.stream.transform

import com.kyrie.stream.source.ClickSource
import org.apache.flink.streaming.api.scala._

object TransReduce {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new ClickSource)
      .map(e => e.user ->1)
      .keyBy(_._1)
      .reduce((r1,r2) => (r1._1 , r1._2 +r2._2)) //计算每个用户访问频次
      .keyBy(_ =>true) //所有数据分到同一个分区
      .reduce((r1,r2) => if(r1._1 > r2._1) r1 else r2)
      .print

    env.execute()

  }

}
