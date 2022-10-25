package com.kyrie.stream.source

import org.apache.flink.streaming.api.scala._
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //通过主机名和端口号读取 socket 文本流
    val lineDS = env.socketTextStream("localhost", 7777)
    // 进行转换计算
    val result = lineDS
      .flatMap(data => data.split(" ")) // 用空格切分字符串
      .map((_, 1)) // 切分以后的单词转换成一个元组
      .keyBy(_._1) // 使用元组的第一个字段进行分组
      .sum(1) // 对分组后的数据的第二个字段进行累加
    // 打印计算结果
    result.print()
    // 执行程序
    env.execute()
  }
}
