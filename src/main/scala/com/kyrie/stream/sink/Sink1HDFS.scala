package com.kyrie.stream.sink

import com.ecommerce.UserBehavior
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

object Sink1HDFS {


  def main(args: Array[String]): Unit = {

    val env =  StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)


    val stream = env.readTextFile("data/ecom/UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserBehavior(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim.toInt,arr(3).trim,arr(4).trim.toLong)
      })

    //默认一个小时一个目录
    //设置分桶策略
    val rollingPolicy:DefaultRollingPolicy[UserBehavior,String] = DefaultRollingPolicy.create()
      .withInactivityInterval(10000)//不活动的分桶时间
      .withRolloverInterval(20000)//每隔多长时间生成文件
      .build()
    //创建sink ;row-encoding format
    val hdfsSink = StreamingFileSink.forRowFormat[UserBehavior](
      new Path("hdfs://192.168.0.61:9000/data/flinktest"),
      new SimpleStringEncoder[UserBehavior]("UTF-8")
    ).withRollingPolicy(rollingPolicy)
      .withBucketCheckInterval(1000) //检查间隔
      .build()

    stream.addSink(hdfsSink)

    env.execute()











  }

}
