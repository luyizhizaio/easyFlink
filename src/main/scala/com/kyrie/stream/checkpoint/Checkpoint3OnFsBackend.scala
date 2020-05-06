package com.kyrie.stream.checkpoint

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._

/*
设置 HDFS 文件系统的状态后端，取消 Job 之后再次恢复 Job
fink 1.9.3版本测试成功
 */
object Checkpoint3OnFsBackend {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(50000)

    //指定hdfs作为状态后端
    env.setStateBackend(new FsStateBackend("hdfs://yarn1:9000/data/flink-checkpoint/cp1"))
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置可允许checkpoint失败次数
    //env.getCheckpointConfig.setTolerableCheckpointFailureNumber(1)


    //读取数据得到DataStream
    val stream = env.socketTextStream("192.168.0.104",8888)
    stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()

    env.execute("Checkpoint3OnFsBackend")

  }

}
