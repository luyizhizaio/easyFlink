package com.kyrie.stream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.Calendar
import scala.util.Random

case class Event(user:String,url:String,timestamp:Long)

class ClickSource extends SourceFunction[Event]{

  var running = true

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val random =new Random()
    val users =Array("ab","sc","ad","lbj","jd")

    val urls = Array("./home","./cart","./fav","./prod?id=1","./prod?id=1","./order?id=23")

    while(running){
      ctx.collect(Event(
        users(random.nextInt(users.length)),
        urls(random.nextInt(urls.length)),
        Calendar.getInstance().getTimeInMillis
      ))

      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = running = false
}
