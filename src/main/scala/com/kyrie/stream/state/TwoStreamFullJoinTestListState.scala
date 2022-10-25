package com.kyrie.stream.state

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 两条流fulljoin
 * 用liststate 保存流数据
 */
object TwoStreamFullJoinTestListState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("a","stream-1",1000L),
      ("b","stream-1",2000L),
    ).assignAscendingTimestamps(_._3)

    val stream2 = env.fromElements(
      ("a","stream-2",3000L),
      ("b","stream-3",4000L),
    ).assignAscendingTimestamps(_._3)

    stream1.keyBy(_._1)
      .connect(stream2.keyBy(_._1))
      .process(new CoProcessFunction[(String,String,Long),
        (String,String,Long),String] {

        lazy val stream1ListState: ListState[(String, String, Long)] = getRuntimeContext.getListState(
          new ListStateDescriptor[(String, String, Long)]("stream1-list", classOf[(String, String, Long)])
        )
        lazy val stream2ListState: ListState[(String, String, Long)] = getRuntimeContext.getListState(
          new ListStateDescriptor[(String, String, Long)]("stream2-list", classOf[(String, String, Long)])
        )

        override
        def processElement1(left: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context,
                            collector: Collector[String]): Unit = {

          stream1ListState.add(left)
          import scala.collection.JavaConversions._

          for (right <- stream2ListState.get()){
          collector.collect(left + "=>"+ right)
          }

        }

        override
        def processElement2(right: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context,
                            collector: Collector[String]): Unit = {
          stream2ListState.add(right)
          import scala.collection.JavaConversions._

          for (left <- stream1ListState.get()){
            collector.collect(right + "=>"+ left)
          }
        }
      }).print()

    env.execute()
  }



}
