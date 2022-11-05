package com.kyrie2

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object RichFunctionTest2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val ds = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L)
    )

    ds.keyBy(_.timestamp).map(new RichMapFunction[Event,String] {

      var valueState:MapState[String,String] = _
      override def open(parameters: Configuration): Unit = {
        valueState = getRuntimeContext.getMapState(
          new MapStateDescriptor[String,String]("what", classOf[String],classOf[String]))
        println("index:"+getRuntimeContext.getIndexOfThisSubtask + "start")
        println("index:"+getRuntimeContext.getIndexOfThisSubtask + "start")
      }

      override def close(): Unit ={
        println("index" +getRuntimeContext.getIndexOfThisSubtask +"end")

        valueState.entries().forEach(x =>{
          val y = x.getKey+ ":" +x.getValue
          println("close:"+ y)
        })

      }

      override def map(in: Event): String = {

        val s = valueState.get(in.user)

        if(s == null){
          println( in.user +">a< -> " + s )
          valueState.put(in.user, in.url)
        }else{
          println( in.user +">b< -> " + s )
          valueState.put(in.user, s + "-" +in.url)
        }

        in.url +"&&"+ in.user
      }
    })
      .print()

    env.execute()

  }
}
