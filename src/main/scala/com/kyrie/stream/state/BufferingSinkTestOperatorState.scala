package com.kyrie.stream.state

import com.kyrie.stream.source.{ClickSource, Event}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object BufferingSinkTestOperatorState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .addSink(new BufferingSink(10))

    env.execute()

  }

  //实现 SinkFunction 和 CheckpointedFunction 这两个接口
  class BufferingSink(threshold: Int) extends SinkFunction[Event]
  with CheckpointedFunction{

    private var checkpointedState:ListState[Event]=_

    private val bufferedElements = ListBuffer[Event]()


//// 每来一条数据调用一次 invode()方法
    override def invoke(value: Event, context: SinkFunction.Context): Unit = {
      bufferedElements +=value

      if(bufferedElements.size == threshold){
        for(element <- bufferedElements){
          //批量输出
          println(element)
        }
        println("-------------输出完毕------------")
      }

    }

    override
    def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
      checkpointedState.clear()

      for(element <- bufferedElements){
        checkpointedState.add(element)
      }
    }

    override
    def initializeState(context: FunctionInitializationContext): Unit = {

      val descriptor = new ListStateDescriptor[Event](
        "buffered-elements",
        classOf[Event]
      )

      // 初始化状态变量
      checkpointedState = context.getOperatorStateStore.getListState(descriptor)

      // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
      if(context.isRestored){
        import scala.collection.JavaConversions._

        for(element <- checkpointedState.get()){
          bufferedElements+=element
        }

      }


    }
  }

}
