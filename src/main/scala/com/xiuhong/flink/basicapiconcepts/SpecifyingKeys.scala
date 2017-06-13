package com.xiuhong.flink.basicapiconcepts

import org.apache.flink.api.scala._

/**
  * author:xiuhong
  * date:6/7/17
  * time:21:39:25
  */
//批量处理
object SpecifyingKeys {
  def main(args: Array[String]): Unit = {
    //1.获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.加载/创建初始数据
    val lines = env.readTextFile("/home/hadoop/person.txt")

    //3.转化数据
    val words = lines.flatMap{_.split(" ")}.map(x=>(x,1))
    val count = words.groupBy(0).sum(1)

    //4.指定计算结果的位置
    count.writeAsText("out")
    //触发执行
    env.execute()
  }
}
