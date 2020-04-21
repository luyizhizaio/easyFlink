package com.kyrie.stream.checkpoint

import java.util.concurrent.TimeUnit

import com.kyrie.stream.watermark.Feedback2
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

/**
 * 检查点demo
 */
object CheckPoint1Memo {

  def main(args: Array[String]): Unit = {

    """
      |checkpoint 存储在 JobManager 的内存中。
      |检查点：存储某个数据在所有任务处理完成之后的状态。
      |checkpoint：可以设置是否外部持久化。如果外部持久化，任务失败checkpoint不会被清理掉。
      |
      |""".stripMargin

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 启动checkpoint，指定checkpoint的时间间隔(毫秒)和语义(默认exactly_once)
    env.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE)
    //指定checkpoint失败，任务是否失败，默认是true
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)

    //设置状态后端：
    env.setStateBackend(new FsStateBackend("file:///Users/jiangyuanyuan/changyue/lcycode/gitHub/easyFlink/data/ckpt/ckpt1") )

    //设置重启策略,重启次数，重启间隔
    //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))

    val stream = env.socketTextStream("localhost",9999)

    val keyStream = stream.map{line =>
      val Array(id,timestamp,fre) = line.split(" ")
      Feedback2(id,timestamp.toLong, fre.toInt)
    }.keyBy(_.id)

    keyStream.print("key:")


    env.execute()


  }

}
