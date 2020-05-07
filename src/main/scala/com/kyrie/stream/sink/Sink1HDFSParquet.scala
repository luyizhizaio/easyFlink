package com.kyrie.stream.sink

import com.ecommerce.UserBehavior
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{OnCheckpointRollingPolicy, DefaultRollingPolicy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用批量编码输出格式:输出到parquet格式
 * 注意：批量编码格式只能和 `OnCheckpointRollingPolicy` 结合使用，每次做 checkpoint 时滚动文件。
 */
object Sink1HDFSParquet {


  def main(args: Array[String]): Unit = {

    val env =  StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.enableCheckpointing(1000)

//    System.setProperty("HADOOP_USER_NAME", "hadoop")


    val stream = env.readTextFile("data/ecom/UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserBehavior(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim.toInt,arr(3).trim,arr(4).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(10)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000
      })
      .keyBy(_.userId)
      .timeWindow(Time.seconds(15))
    .reduce{(a,b) => a}

    stream.print("stream:")

    val bucketAssigner = new DateTimeBucketAssigner[UserBehavior]()


    //创建sink;使用批量编码输出格式:输出到parquet格式
    val hdfsSink = StreamingFileSink.forBulkFormat[UserBehavior](
      new Path("data/flinktest"),
      ParquetAvroWriters.forReflectRecord(classOf[UserBehavior])
    ).withBucketAssigner(bucketAssigner)
      .build()

    stream.addSink(hdfsSink)

    env.execute()











  }

}
