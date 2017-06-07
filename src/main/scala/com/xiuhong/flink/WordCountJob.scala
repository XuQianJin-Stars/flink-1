package com.xiuhong.flink

import org.apache.flink.api.scala._


object WordCountJob {
  def main(args: Array[String]) {
    // 1.设置运行环境
   // val env = ExecutionEnvironment.createLocalEnvironment()
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.创造测试数据
    val text = env.readTextFile("/home/hadoop/person.txt")

    //3.进行wordcount运算
    val counts = text.flatMap(_.split(" "))
      .map((_, 1)).groupBy(0).sum(1)

    //4.打印测试结构
    counts.print()
    env.execute("WordCountJob")
  }
}