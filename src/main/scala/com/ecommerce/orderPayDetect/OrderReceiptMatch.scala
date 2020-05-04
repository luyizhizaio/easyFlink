package com.ecommerce.orderPayDetect

import com.ecommerce.{OrderItem, ReceiptLog}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 支付订单与收据对账,将未匹配上的输出到侧输出流
 */
object OrderReceiptMatch {

  val onlyPaypOutputTag = new OutputTag[OrderItem]("onlyPay")
  val onlyReceiptOutputTag = new OutputTag[ReceiptLog]("onlReceipt")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val payStream = env.readTextFile("data/ecom/OrderLog.csv")
      .map{line =>
        val arr = line.split(",")
        OrderItem(arr(0).trim.toLong,arr(1).trim,arr(2).trim,arr(3).trim.toLong)
      }
      .filter(_.txId !="")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)


    val receiptStream = env.readTextFile("data/ecom/ReceiptLog.csv")
      .map{line =>
        val arr = line.split(",")
        ReceiptLog(arr(0).trim,arr(1).trim,arr(2).trim.toLong)
      }
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)

    val processStream = payStream.connect(receiptStream) //链接连个流
      .process(new PayReceiptMatchProcessFunction()) //处理合并流


    processStream.print("matched")
    processStream.getSideOutput(onlyPaypOutputTag).print("onlyPay")

    processStream.getSideOutput(onlyReceiptOutputTag).print("onlyReceipt")

    env.execute()

  }

  class PayReceiptMatchProcessFunction() extends CoProcessFunction[OrderItem,ReceiptLog,(OrderItem,ReceiptLog)]{

    lazy val payState = getRuntimeContext.getState(new ValueStateDescriptor[OrderItem]("payState",classOf[OrderItem]))
    lazy val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptLog]("receiptState",classOf[ReceiptLog]))


    override
    def processElement1(pay: OrderItem, context: CoProcessFunction[OrderItem, ReceiptLog, (OrderItem, ReceiptLog)]#Context, collector: Collector[(OrderItem, ReceiptLog)]): Unit = {

      val receipt = receiptState.value()
      if(receipt!=null){ //匹配到
        collector.collect((pay,receipt))
        receiptState.clear()
      }else{ //未匹配到
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)//watermark 延迟5秒触发timer
      }


    }

    override
    def processElement2(receipt: ReceiptLog, context: CoProcessFunction[OrderItem, ReceiptLog, (OrderItem, ReceiptLog)]#Context, collector: Collector[(OrderItem, ReceiptLog)]): Unit = {
      val pay = payState.value()
      if(pay!=null){ //匹配到
        collector.collect((pay,receipt))
        payState.clear()
      }else{ //未匹配到
        receiptState.update(receipt)
        context.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)//watermark 延迟5秒触发timer
      }

    }

    override
    def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderItem, ReceiptLog, (OrderItem, ReceiptLog)]#OnTimerContext, out: Collector[(OrderItem, ReceiptLog)]): Unit = {
      val receipt = receiptState.value()
      if(receipt != null){ //收到支付，未收到收据
        ctx.output(onlyReceiptOutputTag,receipt)
        receiptState.clear()
      }

      val pay = payState.value()
      if(pay != null){

        //侧输出流输出报警
        ctx.output(onlyPaypOutputTag,pay)
        payState.clear()
      }

    }
  }

}
