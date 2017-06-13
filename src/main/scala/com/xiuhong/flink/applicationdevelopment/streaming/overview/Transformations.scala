package com.xiuhong.flink.applicationdevelopment.streaming.overview

import org.apache.flink.streaming.api.scala._

/**
  * author:xiuhong
  * date:6/12/17
  * time:08:02:22
  */
object Transformations {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //读入一个元素，返回转换后的一个元素。一个把输入流转换中的数值翻倍的map function：
    // val num = env.fromElements(1, 2, 3, 4)
    // val result = num.map(_ * 2)

    //读入一个元素，返回转换后的0个、1个或者多个元素。一个将句子切分成单词的flatmap function：
    //    val text = env.fromElements("i love you yexing")
    //    val words = text.flatMap { str => str.split(" ") }
    //    words.print()

    //读入的每个元素执行boolean函数，并保留返回true的元素。一个过滤掉零值的filter：
    //    val num = env.fromElements(1, 2, 3, 4)
    //    val result = num.filter(x => x > 2)
    //    result.print()
    //    env.execute()


    //逻辑上将流分区为不相交的分区，每个分区包含相同key的元素。
    // 在内部通过hash分区来实现。关于如何指定分区的keys请参阅keys。
    // 该transformation返回一个KeyedDataStream。
    //    val text = env.fromElements("i love you yexing i love you jeffdean")
    //    val words = text.flatMap { str => str.split(" ") }
    //      .map(x => (x, 1))
    //      .keyBy(0)
    //    words.print()

  }
}
