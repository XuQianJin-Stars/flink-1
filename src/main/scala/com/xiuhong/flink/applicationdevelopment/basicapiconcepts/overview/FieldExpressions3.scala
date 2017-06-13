package com.xiuhong.flink.applicationdevelopment.basicapiconcepts.overview

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration


/**
  * author:xiuhong
  * date:6/11/17
  * time:16:02:28
  */
case class WordCount(word: String, count: Int)

object FieldExpressions3 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("/home/hadoop/person.txt")

    //    val count = text.flatMap(_.split(" "))
    //      .map(x => WordCount(x, 1))
    //      .groupBy("word")
    //      .sum("count")
    //    val count = text.flatMap(_.split(" "))
    //      .map(x => WordCount(x, 1))
    //      .groupBy(_.word)
    //      .sum("count")

  }
}
