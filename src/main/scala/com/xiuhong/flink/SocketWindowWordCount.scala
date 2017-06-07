package com.xiuhong.flink

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * author:xiuhong
  * date:6/6/17
  * time:20:12:38
  */
case class WordWithCount(word: String, count: Long)

object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("hadoop04", port)

    val windowCount = text
      .flatMap(_.split(" "))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count").map(x => x.toString)

    //windowCount.print().setParallelism(2)
    // env.execute()
  }
}
