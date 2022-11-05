package com.kyrie2

import org.apache.flink.api.scala._

object BatchWordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.readTextFile("data/src.txt")

    val sumDs = ds.flatMap(_.split(" "))
      .map(w => (w,1))
      .groupBy(0)
      .sum(1)

    sumDs.print()



    //env.execute("first batch")
  }

}
