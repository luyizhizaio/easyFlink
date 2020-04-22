package com.kyrie.stream.checkpoint

import java.util.Properties

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception.StreamException
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
 * demo: Flink+Kafka 如何实现端到端的 exactly-once 语义
 * 实现数字相加的状态一致性
 */
object CheckPoint2Kafka {

  """
    |端到端的状态一致性的实现，需要每一个组件都实现。
    |
    |flink 内部 —— 利用 checkpoint 机制，把状态存盘，发生故障的时候可以恢复，保证内部的状态一致性
    |
    |source —— kafka consumer 作为 source，可以将偏移量保存下来，如果后
    |续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，
    |保证一致性
    |sink —— kafka producer 作为 sink，采用两阶段提交 sink，需要实现一个
    |TwoPhaseCommitSinkFunction
    |
    |""".stripMargin

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //checkpoint
    env.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)
    //设置状态后端：
    env.setStateBackend(new FsStateBackend("file:///Users/jiangyuanyuan/changyue/lcycode/gitHub/easyFlink/ckpt/kafka") )


    //kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    //properties.setProperty("auto.offset.reset", "latest")


    //定义 kafka topic，数据类型
    val stream:DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("feedback",new SimpleStringSchema(),properties))



    val splitStream = stream.map{line =>
      val Array(k,num) = line.split(",")
      k ->num.toInt
    }.split{tu =>
      if(tu._2 % 2 == 0) Seq("even") else Seq("odd")
    }
    splitStream.print("split:")
    val evenStream:DataStream[(String,Int)] = splitStream.select("even")
    val oddStream:DataStream[(String,Int)] = splitStream.select("odd")

    val evenSumStream =evenStream.keyBy(_._1).flatMap(new AddFunction())
    val oddSumStream =oddStream.keyBy(_._1).flatMap(new AddFunction())

    evenSumStream.print("even:")
    oddSumStream.print("odd:")

    env.execute()


  }

}

class AddFunction extends RichFlatMapFunction[(String,Int),(String,Int)]{

  private var sumState:ValueState[Int] =_

  override def open(parameters: Configuration): Unit = {

    val descriptor = new ValueStateDescriptor[Int]("lastTemp",classOf[Int])

    sumState = getRuntimeContext.getState[Int](descriptor)
  }

  override def flatMap(value: (String,Int), out: Collector[(String,Int)]): Unit = {

    val newValue = sumState.value() + value._2

    sumState.update(newValue)

    out.collect((value._1,newValue))

  }
}
