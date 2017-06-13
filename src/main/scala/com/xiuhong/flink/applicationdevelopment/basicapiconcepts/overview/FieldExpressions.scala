package com.xiuhong.flink.applicationdevelopment.basicapiconcepts.overview


import org.apache.flink.streaming.api.scala._

/**
  * author:xiuhong
  * date:6/11/17
  * time:15:09:52
  */
//注意：经过测试1.3不支持class类，只支持case class类
case class WC(var word: String, var count: Int)

object FieldExpressions {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val words = env.fromElements(WC("zhangsan", 1), WC("jeffdean", 1), WC("jeffdean", 1))

    //val wordCounts = words.keyBy(0).sum(1)
    val wordCounts = words.keyBy("word").sum("count")
    wordCounts.print()
    env.execute()
  }
}
