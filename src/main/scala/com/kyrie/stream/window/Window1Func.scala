package com.kyrie.stream.window

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object Window1Func {

  def main(args: Array[String]): Unit = {


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //默认是 processing time ；修改为 event time.
    //注意：Event Time 的使用一定要指定数据源中的时间戳。否则程序无法知道事件的事件时间是什么(数据源里的数据没有时间戳的话，就只能使用 Processing Time 了)。
   // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //定义 kafka topic，数据类型
    val stream:DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("feedback",new SimpleStringSchema(),properties))

    TimeTumblingWindow(stream)



    env.execute("window function")


  }


  /**
   * 滚动窗口
   */
  def TimeTumblingWindow(stream:DataStream[String]): Unit ={

    /**
     * 输出最小温度
     */
    stream.flatMap{line =>
      println("line:"+line)
      if (line != null && line !=""){
        val Array(id,temp) = line.split(" ")
        Some(id -> temp.toInt)
      }else None
    }.keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
      .print("TumblingWindow")

  }


  /**
   * 滑动窗口
   */
  def TimeSlidingWindow(stream:DataStream[String]): Unit ={

    /**
     * 输出最小温度
     */
    stream.flatMap{line =>
      println("line:"+line)
      if (line != null && line !=""){
        val Array(id,temp) = line.split(" ")
        Some(id -> temp.toInt)
      }else None
    }.keyBy(_._1)
      .timeWindow(Time.seconds(20),Time.seconds(10))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
      .print("sliding")

  }


  /**
   * 数量滑动窗口
   */
  def countSlidingWindow(stream:DataStream[String]): Unit ={

    /**
     * 输出最小温度
     */
    stream.flatMap{line =>
      println("line:"+line)
      if (line != null && line !=""){
        val Array(id,temp) = line.split(" ")
        Some(id -> temp.toInt)
      }else None
    }.keyBy(_._1)
      .countWindow(10,5)
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
      .print("sliding")

  }



}
