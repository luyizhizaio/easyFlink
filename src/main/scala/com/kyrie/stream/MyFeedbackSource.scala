package com.kyrie.stream

import com.kyrie.stream.Source1FromCollection.Feedback
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * 自定义source
 */
class MyFeedbackSource extends SourceFunction[Feedback] {


  var running: Boolean = true

  /**
   * run 开始source，发射元素
   * @param ctx
   */
  override
  def run(ctx: SourceFunction.SourceContext[Feedback]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()
    var curTemp = 1.to(10).map(i =>
      ( "sensor_" + i, 65 + rand.nextGaussian() * 20 )
    )
    var n = 0
    while(running){
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian() )
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        //Emits one element from the source, without attaching a timestamp.
        t => ctx.collect(Feedback(t._1, curTime+"", t._2+""))
      )
      Thread.sleep(100)
      n+=1
      if(n == 3){
        println("execute cancel")
        this.cancel() //调用cancel 任务终止。

      }
    }
  }


  /**
   * cancel 终止source
   */
  override
  def cancel(): Unit = {
    running = false
  }
}
