package com.xiuhong.flink.cookbook.dataset.chapter09

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * author:xiuhong
  * date:6/16/17
  * time:14:14:38
  */
object Operator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //join
    //一个或多个field 位置keys(仅仅Tuple DataSets)
    //    val input1: DataSet[(Int, String)] = env.fromElements(
    //      (2, "zhagnsan"), (3, "lisi"), (4, "wangwu"), (5, "zhaoliu"))

    //2.创建一个 DataSet其元素为[(Double, Int)]类型
    //    val input2: DataSet[(Double, Int)] = env.fromElements(
    //      (1850.98, 4), (1950.98, 5), (2350.98, 6), (3850.98, 3))

    //3.两个DataSet进行join操作，条件是input1(0)==input2(1)
    //    val result = input1.join(input2).where(0).equalTo(1)
    //    println(result.collect())


    //用join函数来进行join
    //    case class Rating(name: String, category: String, points: Int)
    //    //定义DataSet[Rating]
    //    val ratings: DataSet[Rating] = env.fromElements(
    //      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
    //      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
    //    //创建DataSet[(String, Double)]
    //    val weights: DataSet[(String, Double)] = env.fromElements(
    //      ("youny1", 4.3), ("youny2", 7.2),
    //      ("youny3", 9.0), ("youny4", 1.5))
    //    val result = ratings.join(weights).where("category").equalTo(0) {
    //      (ratings, weights) => (ratings.name, ratings.points + weights._2)
    //    }
    //    println(result.collect())

    //用flat－join函数进行join
    //    case class Rating(name: String, category: String, points: Int)
    //    val ratings: DataSet[Rating] = env.fromElements(
    //      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
    //      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))
    //
    //    val weights: DataSet[(String, Double)] = env.fromElements(
    //      ("youny1", 4.3), ("youny2", 7.2),
    //      ("youny3", 9.0), ("youny4", 1.5))
    //
    //    val weightedRatings = ratings.join(weights).where("category").equalTo(0) {
    //      (rating, weight, out: Collector[(String, Double)]) =>
    //        if (weight._2 > 0.1) out.collect(rating.name, rating.points * weight._2)
    //    }
    //    println(weightedRatings.collect())


    //带数据集大小提示的join   为了帮助优化器选择正确的执行策略， 用户可以提示join的数据集的大小：

    //    val input1 = env.fromElements((3, "zhangsan"), (2, "lisi"), (4, "wangwu"), (6, "zhaoliu"))

    //2.定义 DataSet[(Int, String)]
    //    val input2 = env.fromElements((4000, "zhangsan"), (70000, "lisi"), (4600, "wangwu"), (53000, "zhaoliu"))
    //暗示第二个输入很小
    //    val result = input1.joinWithTiny(input2).where(1).equalTo(1)
    //    println(result.collect())

    // 暗示第二个输入很大
    //    val result2 = input1.joinWithHuge(input2).where(1).equalTo(1)
    //    println(result2.collect())


    //join算法提示
    /**
      * 有如下提示：
      * OPTIMIZER_CHOOSES: 等价于没有提示， 让系统做选择。
      * BROADCAST_HASH_FIRST: 广播第一个数据集并根据它来创建一张hash table，
      * 这个hash table 会被第二个数据集来调查（probe）。 这种策略适合第一个数据集比较小。
      * BROADCAST_HASH_SECOND: 广播第二个数据集并根据它来创建一张hash table， 这个hash
      * table 会被第一个数据集来调查（probe）。 这种策略适合第二个数据集比较小。
      * REPARTITION_HASH_FIRST: 系统分区（shuffles）每一个输入（除非这个输入已经被分区）
      * 并根据第一个数据集创建一张hash table。 这种策略适合第一个数据集比第二个数据集小，但2
      * 个数据集仍然很大。 注意： 这是默认的fallback策略， 当无法评估大小时并且没有预先存在的
      * parition和可以重复使用的sort－order。
      * REPARTITION_HASH_SECOND: 系统分区（shuffles）每一个输入（除非这个输入已经被分区）
      * 并根据第二个数据集创建一张hash table。 这种策略适合第一个数据集比第二个数据集大，但2个
      * 数据集仍然很大。
      * REPARTITION_SORT_MERGE: 系统分区（shuffles）每一个输入（除非这个输入已经被分区）并对
      * 每个数据集进行排序（除非它已经排序了） 2个数据集通过merge 排序的数据集， 这种策略非常适合，
      * 当一个或2个数据集都已经排好序。
      */
    //    val input1 = env.fromElements((3, "zhangsan"), (2, "lisi"), (4, "wangwu"), (6, "zhaoliu"))
    //    val input2 = env.fromElements((4000, "zhangsan"), (70000, "lisi"), (4600, "wangwu"), (53000, "zhaoliu"))

    //2.暗示input2很小
    //    val result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST).where(1).equalTo(1)

    //    println(result.collect())


    //cross 交叉转换（Croass）合并2个数据集到一个数据集。
    //    val coords1 = env.fromElements((1, 4, 7), (2, 5, 8), (3, 6, 9))
    //    val coords2 = env.fromElements((10, 40, 70), (20, 50, 80), (30, 60, 90))

    //2.交叉两个DataSet[Coord]
    //    val result1 = coords1.cross(coords2)
    //    result1.print()


    //1.定义 case class
    //    case class Coord(id: Int, x: Int, y: Int)

    //2.定义两个DataSet[Coord]
    //    val coords1: DataSet[Coord] = env.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    //    val coords2: DataSet[Coord] = env.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))

    //3.交叉两个DataSet[Coord]，使用自定义方法
    //    val r = coords1.cross(coords2) {
    //      (c1, c2) => {
    //        val dist = (c1.x + c2.x) + (c1.y + c2.y)
    //        (c1.id, c2.id, dist)
    //      }
    //    }


    //Cross with DataSet Size Hint为了帮助优化器选择正确的执行策略， 可以提示数据集的大小给cross函数，
//    case class Coord(id: Int, x: Int, y: Int)
//
//    val coords1 = env.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
//    val coords2 = env.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))
//    //交叉两个DataSet[Coord]，暗示第二个输入较小
//    val result = coords1.crossWithTiny(coords2)
//    result.print()
//
//


  }
}
