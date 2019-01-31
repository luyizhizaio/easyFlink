package com.kyrie.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Created by tend on 2019/1/31.
 *
 */
object WordCount {

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)

    //开始环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    //
    env.getConfig.setGlobalJobParameters(params)

    val text =
      if(params.has("input")){
        env.readTextFile(params.get("input"))
      } else{
        //利用数组创建DataSet
        env.fromCollection(List("nihao scala","hello flink","fuck world"))

      }
    //业务处理
    val counts = text.flatMap{_.toLowerCase.split("\\W+") filter{_.nonEmpty}}
      .map{(_,1)}
      .groupBy(0)//根据inde=0的字段分组
      .sum(1) //根据inde=1的字段sum


    if(params.has("output")){
      //参数： 输出目录，行分隔，字段间分隔
      counts.writeAsCsv(params.get("output"),"\n"," ")

      //启动任务
      env.execute("Scala WordCount Example")

    }else{
      println("Printing result to stdout......")

      counts.print()

    }

  }

}
