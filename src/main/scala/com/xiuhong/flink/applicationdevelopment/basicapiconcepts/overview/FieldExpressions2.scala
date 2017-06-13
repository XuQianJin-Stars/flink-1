package com.xiuhong.flink.applicationdevelopment.basicapiconcepts.overview

import com.sun.org.apache.bcel.internal.generic.NEW
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.io.IntWritable

/**
  * author:xiuhong
  * date:6/11/17
  * time:15:31:02
  */
case class WC1(var complex: ComplexNestedClass, var count: Int)

//注意：类参数需要声明是var或者val
class ComplexNestedClass(
                          var someNumber: Int,
                          val someFloat: Float,
                          val word: (Long, Long, String)
                        ) {
  def this() {
    this(0, 0, (0, 0, ""))
  }
}

object FieldExpressions2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val word = env.fromElements {
      WC1(new ComplexNestedClass(1, 1f, (1, 1, "a")), 1)
      WC1(new ComplexNestedClass(1, 1f, (1, 1, "a")), 1)

    }
    val count = word.keyBy(_.complex.word._3).sum("count")
    count.print()
    env.execute()
  }
}
