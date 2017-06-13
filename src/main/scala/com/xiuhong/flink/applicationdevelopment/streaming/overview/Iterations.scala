package com.xiuhong.flink.applicationdevelopment.streaming.overview

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * author:xiuhong
  * date:6/12/17
  * time:12:57:28
  */
object Iterations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val someIntegers: DataStream[Long] = env.generateSequence(0, 10)

    val iteratedStream = someIntegers.iterate(
      iteration => {
        val minusOne = iteration.map(v => v - 1)
        val stillGreaterThanZero = minusOne.filter(_ > 0)
        val lessThanZero = minusOne.filter(_ <= 0)
        (stillGreaterThanZero, lessThanZero)
      }
    )
    iteratedStream.print()
    env.execute()
  }
}
