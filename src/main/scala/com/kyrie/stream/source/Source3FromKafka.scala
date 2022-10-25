package com.kyrie.stream.source

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Source3FromKafka {
  case class WordWithCount(word:String,count:Int)

  def main(args: Array[String]): Unit = {

    /*
    kafka创建生产者：
    bin/kafka-console-producer.sh --broker-list 192.168.0.104:9092 --topic feedback
     */



    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //定义 kafka topic，数据类型
    val stream:DataStream[String] = env.addSource(
      new FlinkKafkaConsumer[String]("feedback",new SimpleStringSchema(),properties))

    stream.flatMap{line => line.split("\\s")}
      .map{w => WordWithCount(w,1)}.keyBy("word").sum("count").print()

    env.execute("little boy")

  }

}
