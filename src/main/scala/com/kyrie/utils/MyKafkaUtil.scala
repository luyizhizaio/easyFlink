package com.kyrie.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MyKafkaUtil {


  def getConsumer(topicName:String)={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    new FlinkKafkaConsumer011[String](topicName,new SimpleStringSchema(),properties)

  }

}
