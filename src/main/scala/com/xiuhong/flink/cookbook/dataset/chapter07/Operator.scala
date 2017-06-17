package com.xiuhong.flink.cookbook.dataset.chapter07

import com.xiuhong.flink.applicationdevelopment.basicapiconcepts.overview.WC
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * author:xiuhong
  * date:6/15/17
  * time:14:36:51
  */
object Operator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // print 打印结果
    //    val input = env.fromElements("A", "B", "C")
    //    input.print()

    //count  计算DataSet中元素的个数
    //    val input = env.fromElements("A", "B", "C")
    //    println(input.count)


    //min 获取最小的元素
    //    case class Student(age: Int, name: String, hight: Double)
    //
    //    val input: DataSet[Student] = env.fromElements(
    //      Student(16, "zhangasn", 194.5),
    //      Student(17, "zhangasn", 184.5),
    //      Student(18, "zhangasn", 174.5),
    //      Student(16, "lisi", 194.5),
    //      Student(17, "lisi", 184.5),
    //      Student(18, "lisi", 174.5))
    //2.获取age最小的元素 其实是所有字段值最小的组合
    //input.min(0).print()
    //input.min("age").print()

    //sum 获取元素的累加和，只能作用于数值类型
    //    case class Wc(key: String, count: Int)
    //    val input = env.fromElements(Wc("jeffdean", 1), Wc("jeffdean", 1))
    //    input.groupBy("key").sum(1).print()


    //getType 获取DataSet的元素的类型信息
    //    val input: DataSet[String] = env.fromElements("A", "B", "C")
    //
    //    println(input.getType())

    //map 将一个DataSet转化成另一个DataSet。转化操作对每一个元素执行一次。
    // val input: DataSet[Int] = env.fromElements(23, 67, 18, 29, 32, 56, 4, 27)
    //2.将DataSet中的每个元素乘以2
    //val result = input.map(_ * 2)

    //1.创建一个DataSet[(Int, Int)]
    //    val intPairs: DataSet[(Int, Int)] = env.fromElements((18, 4), (19, 5), (23, 6), (38, 3))
    //2.键值对的key+value之和生成新的dataset
    //    val intSums = intPairs.map { pair => pair._1 + pair._2 }
    //    intSums.print()


    //flatMap
    //1.创建一个 DataSet其元素为String类型
    //    val input: DataSet[String] = env.fromElements("zhangsan boy", "lisi girl")
    //
    //2.将DataSet中的每个元素用空格切割成一组单词
    //    val result = input.flatMap {
    //      _.split(" ")
    //    }
    //    result.print()


    //mapPartition 和map类似，不同它的处理单位是partition，而非element。
    //1.创建一个 DataSet其元素为String类型
    //    val input: DataSet[String] = env.fromElements("zhangsan boy", "lisi is a girl so sex")

    //2.获取partition的个数
    //    val result = input.mapPartition { in => Some(in.size) }
    //    result.print()


    //filter 过滤满足添加的元素，不满足条件的元素将被丢弃！
    //过滤偶数
    //    val input = env.fromElements(1, 2, 3, 4, 5)
    //    val result = input.filter(_ % 2 == 0)
    //    result.print()

    //reduce 根据一定的条件和方式来合并DataSet
    //Int类型的DataSet做reduce
    //    val a: DataSet[Int] = env.fromElements(2,5,9,8,7,3)
    //    val b: DataSet[Int] = a.reduce { _ + _ }
    //    b.print()

    //String类型的DataSet做reduce
    //    val a: DataSet[String] = env.fromElements("zhangsan boy", " lisi girl")
    //    val b: DataSet[String] = a.reduce {
    //      _ + _
    //    }
    //    b.print()


    //groupby
    //1.定义 class
    //    case class WC(val word: String, val salary: Int)
    //2.定义DataSet[WC]
    //    val words: DataSet[WC] = env.fromElements(
    //      WC("LISI", 600), WC("LISI", 400), WC("WANGWU", 300), WC("ZHAOLIU", 700))
    //    val wordcount = words.groupBy("word").reduce((w1, w2) => {
    //      new WC(w1.word, w1.salary + w2.salary)
    //    })
    //    wordcount.print()

    //使用自定义的reduce方法,使用key-selector
    //    val wordcount = words.groupBy(_.word).reduce((w1, w2) => {
    //      new WC(w1.word, w1.salary + w2.salary)
    //    })
    //    wordcount.print()


    //    groupBy示例二：使用多个Case Class Fields 执行程序：
    //1.定义 case class
    //    case class Student(val name: String, addr: String, salary: Double)

    //2.定义DataSet[Student]
    //    val tuples: DataSet[Student] = env.fromElements(
    //      Student("lisi", "shandong", 2400.00), Student("zhangsan", "henan", 2600.00),
    //      Student("lisi", "shandong", 2700.00), Student("lisi", "guangdong", 2800.00))

    //3.使用自定义的reduce方法,使用多个Case Class Fields name
    //    val reducedTuples1 = tuples.groupBy("name", "addr").reduce {
    //      (s1, s2) => Student(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    //    }

    //4.使用自定义的reduce方法,使用多个Case Class Fields index
    //    val reducedTuples2 = tuples.groupBy(0, 1).reduce {
    //      (s1, s2) => Student(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    //    }

    //5.使用自定义的reduce方法,name和index混用
    //    val reducedTuples3 = tuples.groupBy(0, 1).reduce {
    //      (s1, s2) => Student(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    //    }


    //ReduceGroup 此函数和reduce函数类似，不过它每次处理一个group而非一个元素。
    //    val input: DataSet[(Int, String)] = env.fromElements(
    //      (20, "zhangsan"), (22, "zhangsan"),
    //      (22, "lisi"), (20, "zhangsan"))
    //先分组
    //2.先用string分组，然后对分组进行reduceGroup
    //    val output = input.groupBy(1).reduceGroup {
    //将相同的元素用set去重
    //      (in, out: Collector[(Int, String)]) =>
    //        in.toSet foreach (out.collect)
    //    }
    //    output.print()


    //    ReduceGroup示例二，操作case class
    //1.定义case class
    //    case class Student(age: Int, name: String)

    //2.创建DataSet[Student]
    //    val input: DataSet[Student] = env.fromElements(
    //      Student(20, "zhangsan"),
    //      Student(22, "zhangsan"),
    //      Student(22, "lisi"),
    //      Student(20, "zhangsan"))
    //3.以age进行分组，然后对分组进行reduceGroup
    //    val output = input.groupBy(_.age).reduceGroup {
    //      将相同的元素用set去重
    //      (in, out: Collector[Student]) =>
    //        in.toSet foreach (out.collect)
    //    }
    //    output.print()


    //sortGroup 根据分组排序
    //1.创建 DataSet[(Int, String)]
    //    val input: DataSet[(Int, String)] = env.fromElements(
    //      (20, "zhangsan"),
    //      (22, "zhangsan"),
    //      (22, "lisi"),
    //      (22, "lisi"),
    //      (22, "lisi"),
    //      (18, "zhangsan"),
    //      (18, "zhangsan"))

    //2.用int分组，用int对分组进行排序   升序(ASCENDING)
    //    val sortdata = input.groupBy(0).sortGroup(0, Order.ASCENDING)

    //3.对排序好的分组进行reduceGroup
    //    val outputdata = sortdata.reduceGroup {
    //      (in, out: Collector[(Int, String)]) =>
    //        in.toSet foreach (out.collect)
    //    }
    //    println(outputdata.collect())


    //minBy  在分组后的数据中，获取每组最小的元素。
    //    case class Student(age: Int, name: String, height: Double)

    //2.创建DataSet[Student]
    //    val input: DataSet[Student] = env.fromElements(
    //      Student(16, "zhangasn", 194.5),
    //      Student(17, "zhangasn", 184.5),
    //      Student(18, "zhangasn", 174.5),
    //      Student(16, "lisi", 194.5),
    //      Student(17, "lisi", 184.5),
    //      Student(18, "lisi", 174.5))
    //根据姓名分组 获取每组中年龄和身高值最小的
    //    val out = input.groupBy(_.name).minBy(0, 2)
    //    println(out.collect())


    //maxBy  在分组后的数据中，获取每组最大的元素
    //    case class Student(age: Int, name: String, height: Double)

    //2.创建DataSet[Student]
    //    val input: DataSet[Student] = env.fromElements(
    //      Student(16, "zhangasn", 194.5),
    //      Student(17, "zhangasn", 184.5),
    //      Student(18, "zhangasn", 174.5),
    //      Student(16, "lisi", 194.5),
    //      Student(17, "lisi", 184.5),
    //      Student(18, "lisi", 174.5))

    //    val outData = input.groupBy(_.name).maxBy(0)
    //    println(outData.collect)

    //以name进行分组，获取height和age最大的元素
    //    val outData = input.groupBy(_.name).maxBy(0, 2)
    //    println(outData.collect)


    //distinct
    //单一项目的去重 执行程序：
    //    val input = env.fromElements("lisi", "jeffdean", "lisi", "jeffdean", "yexing")
    //    val result = input.distinct()
    //    println(result.collect())

    //多项目的去重，不指定比较项目，默认是全部比较
    //    val input: DataSet[(Int, String, Double)] = env.fromElements(
    //      (2, "zhagnsan", 1654.5), (3, "lisi", 2347.8), (2, "zhagnsan", 1654.5),
    //      (4, "wangwu", 1478.9), (5, "zhaoliu", 987.3), (2, "zhagnsan", 1654.0))
    //
    //    //2.元素去重:指定比较第0和第1号元素
    //    val output = input.distinct(0, 1)
    //    println(output.collect())

    //distinct示例四，case class的去重，指定比较项目
    //    case class Student(name: String, age: Int)

    //2.创建DataSet[Student]
    //    val input: DataSet[Student] = env.fromElements(
    //      Student("zhangsan", 24), Student("zhangsan", 24), Student("zhangsan", 25),
    //      Student("lisi", 24), Student("wangwu", 24), Student("lisi", 25))

    //去掉age重复的
    //    println(input.distinct("age").collect())
    //去掉name重复的
    //    println(input.distinct("name").collect())
    //去掉name age重复的
    //    println(input.distinct("name", "age").collect())

    //6.去掉name和age重复的元素
    //    val all = input.distinct()
    //    all.collect

    //7.去掉name和age重复的元素
    //    val all0 = input.distinct("_")
    //    all0.collect

    //    distinct示例五，根据表达式进行去重

    //
    //    val input: DataSet[Int] = env.fromElements(3, -3, 4, -4, 6, -5, 7)
    //    val result = input.distinct(x => Math.abs(x))
    //    println(result.collect())


    //join  将两个DataSet进行join操作
    //    val input0 = env.fromElements(("zhangsan", 19), ("lisi", 20), ("yexing", 21),("jeffdean",50))
    //    val input1 = env.fromElements(("zhangsan", 19), ("lisi", 20), ("yexing", 21))
    //    val result = input0.join(input1).where(0).equalTo(0)
    //    println(result.collect())


    //case class 类型的join
    //1.定义case class
    case class Rating(name: String, category: String, points: Int)
    //2.定义DataSet[Rating]
    val ratings: DataSet[Rating] = env.fromElements(
      Rating("moon", "youny1", 3), Rating("sun", "youny2", 4),
      Rating("cat", "youny3", 1), Rating("dog", "youny4", 5))

    //3.创建DataSet[(String, Double)]
    val weights: DataSet[(String, Double)] = env.fromElements(
      ("youny1", 4.3), ("youny2", 7.2),
      ("youny3", 9.0), ("youny4", 1.5))
    val weightedRatings = ratings.join(weights).where("category")
      .equalTo(0) {
        (ratings, weights) => (ratings.name, ratings.points + weights._2)
      }

    println(weightedRatings.collect)


  }
}
