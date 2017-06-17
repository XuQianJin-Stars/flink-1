package com.xiuhong.flink.cookbook.dataset.chatper10

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._

/**
  * author:xiuhong
  * date:6/16/17
  * time:15:59:03
  */
object Opertator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //union 执行2个数据集上的union， 这2个数据集必须类型相同。 超过2个的数据集union可以调用union函数多次。
    //    val input = env.fromElements(("jeffdean", 20), ("musk", 20))
    //    val input1 = env.fromElements(("yexing", 20), ("xiuhong", 20))
    //    val result = input.union(input1)
    //    println(result.collect())


    //rebalance   均匀rebalance一个多分区的数据集科研用来解决数据倾斜。
    //    val in: DataSet[String] = env.fromElements("A", "B", "C")
    //    val out = in.rebalance().map {
    //      _.toLowerCase()
    //    }
    //    out.print()


    //Hash-Partition 在一个给定key上对一个数据集进行hash-partition. 可以用position key， key表达式， key－selector函数来选择key
    //    val in: DataSet[(String, Int)] = env.fromElements(("jeffdean", 20), ("musk", 20))
    //    val out = in.partitionByHash(0).mapPartition { it =>
    //      Some(it.size)
    //    }
    //    out.print()


    //range-partition 在一个给定key上对一个数据集进行range-partition. 可以用position key， key表达式， key－selector函数来选择key
    //    val in: DataSet[(String, Int)] = env.fromElements(("jeffdean", 20), ("musk", 20))
    //    val out = in.partitionByRange(0).mapPartition { it =>
    //      Some(it.size)
    //    }
    //    out.print()


    //    val input: DataSet[(Int, String)] = env.fromElements((1, "zhangsan"), (2, "lisi"), (2, "lisi"))
    //    val output = input.groupBy(1).aggregate(Aggregations.SUM, 0)
    //    println(output.collect())
  }
}
