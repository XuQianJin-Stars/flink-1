package com.xiuhong.flink.applicationdevelopment.basicapiconcepts.overview

import org.apache.flink.streaming.api.scala._

/**
  * author:xiuhong
  * date:6/11/17
  * time:16:43:34
  */
case class WordWithCount(var word: String, var count: Int)

object FiledExpressions4 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = env.fromElements(
      new WordWithCount("hello", 1),
      new WordWithCount("world", 2)) // Case Class Data Set

    val count = input.keyBy("word").sum("count") // key by field expression "word"
    count.print()
    env.execute()
  }
}
