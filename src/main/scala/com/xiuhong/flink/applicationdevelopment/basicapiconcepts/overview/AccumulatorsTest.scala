package com.xiuhong.flink.applicationdevelopment.basicapiconcepts.overview

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author:xiuhong
  * date:6/11/17
  * time:16:59:12
  */


object AccumulatorsTest {
  def main(args: Array[String]) {

    val numLines = new IntCounter()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("5", "4", "3")

    val wm = data.map(new RichMapFunction[String, String]() {

      override def open(parameters: Configuration): Unit = {
        //初始化方法，初始化时候完成加速器的定义
        getRuntimeContext.addAccumulator("daxinCounter", numLines)
      }

      override def map(value: String): String = {

        numLines.add(1)
        ""
      }
    })


    //println(env.getExecutionPlan())

    val rs = env.execute()

    val counter: Int = rs.getAccumulatorResult("daxinCounter")

    println("counter : " + counter)

  }


}
