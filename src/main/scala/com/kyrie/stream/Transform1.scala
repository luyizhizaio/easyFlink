package com.kyrie.stream

import com.kyrie.stream.source.WordWithCount
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

case class Feedback(id:String,idtype:String,num:Int)

object Transform1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


//    reduceFun(dataStream)

//    rollAggregation(env)

//    splitAndSelectFun(env)

//    ConnectAndCoMapFun(env)
    unionFun(env)
    env.execute()

  }

  def unionFun(env:StreamExecutionEnvironment):Unit={

    """
      |union
      |DataStream → DataStream：对两个或者两个以上的 DataStream 进行 union 操
      |作，产生一个包含所有 DataStream 元素的新 DataStream。
      |""".stripMargin

    """
      |Connect 与 Union 区别：
      |1． Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap
      |中再去调整成为一样的。
      |2. Connect 只能操作两个流，Union 可以操作多个。
      |""".stripMargin

    val stream1:DataStream[String] = env.readTextFile("data/src.txt")

    val stream2:DataStream[String] = env.readTextFile("data/src2.txt")

    val x1 = stream1.flatMap(_.split("\\s"))
      .map(w => WordWithCount(w,1))


    val x2 = stream2.map{line =>
      val Array(w,c) = line.split(" ")
     WordWithCount(w,c.toInt)
    }


    x1.union(x2).writeAsCsv("data/out/union",WriteMode.OVERWRITE).setParallelism(1)


  }


  def ConnectAndCoMapFun(env:StreamExecutionEnvironment):Unit={
    """
      |connect
      |
      |DataStream,DataStream → ConnectedStreams：
      |连接两个保持他们类型的数据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持各
      |自的数据和形式不发生任何变化，两个流相互独立。
      |
      |CoMap
      |
      |ConnectedStreams → DataStream：作用于 ConnectedStreams 上，功能与 map
      |和 flatMap 一样，对 ConnectedStreams 中的每一个 Stream 分别进行 map 和 flatMap
      |处理。
      |
      |""".stripMargin

    val dataStream:DataStream[String] = env.readTextFile("data/src2.txt")

    val splitStream = dataStream.map{line =>
      val Array(w,req) = line.split(" ")
      WordWithCount(w,req.toInt)
    }.split{wc =>
      if(wc.count > 2) Seq("high") else Seq("low")
    }

    val high = splitStream.select("high")
    val low = splitStream.select("low")

    val haha = high.map{wc => (wc.word,wc.count)}

    //connect
    val connected:ConnectedStreams[(String,Long),WordWithCount] = haha.connect(low)

    //CoMap
    connected.map(
      highData => (highData._1,highData._2,"high"),
      lowData => (lowData.word,"low")
    ).print("xyz,abc").setParallelism(1)

  }

  def splitAndSelectFun(env:StreamExecutionEnvironment): Unit ={
    """
      |split
      |DataStream → SplitStream：根据某些特征把一个 DataStream 拆分成两个或者
      |多个 DataStream。
      |
      |select
      |SplitStream→DataStream：从一个 SplitStream 中获取一个或者多个 DataStream。
      |
      |""".stripMargin

    /**
     * 需求：传感器数据按照温度高低（以 30 度为界），拆分成两个流。
     */

    val dataStream:DataStream[String] = env.readTextFile("data/src2.txt")

    val splitStream = dataStream.map{line =>
      val Array(w,req) = line.split(" ")
      WordWithCount(w,req.toInt)
    }.split{wc =>
      if(wc.count > 2) Seq("high") else Seq("low")
    }

    val high:DataStream[WordWithCount] = splitStream.select("high")

    high.keyBy("word")
      //.timeWindow(Time.seconds(5))
      .sum("count")
      .print()
      .setParallelism(1)

    val low:DataStream[WordWithCount] = splitStream.select("low")

    low.keyBy("word")
      .sum("count")
      .print()
      .setParallelism(1)


    //合并流
    val all = splitStream.select("high", "low")

    all.map{wc => WordWithCount(wc.word +"_all",wc.count)}
      .keyBy("word")
      .sum("count")
      .print()
      .setParallelism(1)

  }

  /**
   * 滚动聚合算子
   * @param env
   */
  def rollAggregation(env:StreamExecutionEnvironment): Unit ={
    """
      |这些算子可以针对 KeyedStream 的每一个支流做聚合。
      | sum()
      | min()
      | max()
      | minBy()
      | maxBy()
      |""".stripMargin



    val dataStream:DataStream[String] = env.readTextFile("data/src2.txt")

    dataStream.map{line =>
        val Array(w,num) = line.split(" ")
        WordWithCount(w,num.toInt)}
      .keyBy("word")
      .countWindow(5) //相同key的元素数量
      .min("count").print().setParallelism(1)

    dataStream.map{line =>
      val Array(w,num) = line.split(" ")
      WordWithCount("minBy-"+w,num.toInt)}
      .keyBy("word")
      .countWindow(5) //相同key的元素数量
      .minBy("count").print.setParallelism(1)
  }

  def reduceFun(env:StreamExecutionEnvironment): Unit ={
    """
      |reduce
      |KeyedStream → DataStream：一个分组数据流的聚合操作，合并当前的元素
      |和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是
      |只返回最后一次聚合的最终结果。
      |""".stripMargin

    val dataStream:DataStream[String] = env.readTextFile("data/src.txt")

    dataStream.flatMap(_.split("\\s"))
      .map((_,1))
      .keyBy(_._1)
      .reduce((a,b) => (a._1 +"_new" ,a._2 +b._2))
      .print()
      .setParallelism(1) //Sets the parallelism for this sink.

  }


}
