package com.xiuhong.flink

import org.apache.flink.api.scala._

/**
  * author:xiuhong
  * date:6/7/17
  * time:08:24:04
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.readTextFile("/home/hadoop/person.txt")

    val counts = text.flatMap {
      _.split(" ")
    }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)
    //注意：在counts.print()后执行env.execute("")会报错


    //counts.print()
    //注意：保存数据到文件，必须env.execute("")
    counts.writeAsCsv("/home/hadoop/out/", "\n", " ")
    env.execute("Scala WordCount Example")
  }
}
