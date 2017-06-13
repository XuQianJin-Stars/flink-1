package com.xiuhong.flink.basicapiconcepts

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author:xiuhong
  * date:6/7/17
  * time:22:04:03
  */
//流式处理
object SpecifyingKeysDateStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.readTextFile("/home/hadoop/person.txt")

    val wordAndNume = lines.flatMap(_.split(" ")).map(x => (x,1))

    val count = wordAndNume.keyBy(0).timeWindow(Time.seconds(5), Time.seconds(1)).sum(1)

    count.writeAsCsv("out")
    env.execute()
  }
}
