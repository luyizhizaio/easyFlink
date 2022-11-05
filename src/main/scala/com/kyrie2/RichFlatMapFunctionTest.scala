package com.kyrie2

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFlatMapFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L),
      Event("Cary", "./order", 60 * 1000L)
    )

    ds.keyBy(_.user).flatMap(new RichFlatMapFunction[Event,String] {

      var mapState:MapState[String,String] = _

      override def open(parameters: Configuration): Unit = {
        mapState = getRuntimeContext.getMapState(
          new MapStateDescriptor[String,String]("what", classOf[String],classOf[String]))
        println("index:"+getRuntimeContext.getIndexOfThisSubtask + "start")
      }

      override def close(): Unit ={
        val inx =getRuntimeContext.getIndexOfThisSubtask
        //println("index" + inx +"end")
        val iter =mapState.iterator()
        while(iter.hasNext){
          val n =iter.next()
            println(n.getKey +"_" +n.getValue)
        }

//        valueState.entries().forEach(x =>{
//          val y = x.getKey+ ":" +x.getValue
//          println("close:"+inx +":"+ y)
//        })
//
//        println("get:" +valueState.get("Bob"))
//        println("get:"+valueState.get("Alice"))

      }

      override def flatMap(in: Event, collector: Collector[String]): Unit = {

        collector.collect(in.user)

        val s = mapState.get(in.user)

        if(s == null){
          println( "-----" +in.user +"-> " + in.url )
          mapState.put(in.user, in.url)
        }else{
          println( "=====" + in.user +"-> " + s + "-" +in.url )
          mapState.put(in.user, s + "-" +in.url)
        }

      }
    })
      .print()

    env.execute()

  }
}
