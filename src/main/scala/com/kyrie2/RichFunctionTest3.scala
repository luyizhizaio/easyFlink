package com.kyrie2

import com.kyrie.stream.source.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFunctionTest3 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L),
      Event("Alice", "./cart", 60 * 1000L),
      Event("Mary", "./cart", 60 * 1000L),
    )

    ds.keyBy(_.user)
      .process(new KeyedProcessFunction[String, Event, String] {

        var valueState:MapState[String,String] = _
        override def open(parameters: Configuration): Unit = {
          valueState = getRuntimeContext.getMapState(
            new MapStateDescriptor[String,String]("what", classOf[String],classOf[String]))
          println("index:"+getRuntimeContext.getIndexOfThisSubtask + "start")
        }

        override def processElement(in: Event, context: KeyedProcessFunction[String, Event, String]#Context,
                                    collector: Collector[String]): Unit ={

          val cukey = context.getCurrentKey

          val s = valueState.get(cukey)

          if(s == null){
            //println( cukey + "--"+ in.user +" -> " + in.url )
            valueState.put(in.user, in.url)
            collector.collect(in.user +"->"+ in.url)
          }else{
            //println( cukey + "===" +in.user +" -> " + in.url )
            valueState.put(in.user, s + "-" +in.url)
            collector.collect(in.user +"->"+ s + "-" +in.url)
          }


        }

        override def close(): Unit ={
          println("index" +getRuntimeContext.getIndexOfThisSubtask +"end")
          valueState.entries().forEach(x =>{
            val y = x.getKey+ ":" +x.getValue
            println("close:"+ y)
          })

        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext,
                             out: Collector[String]): Unit = {


        }
      }).print()

    env.execute()

  }
}
