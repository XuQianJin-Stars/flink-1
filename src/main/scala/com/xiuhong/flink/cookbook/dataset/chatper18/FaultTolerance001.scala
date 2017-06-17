package com.xiuhong.flink.cookbook.dataset.chatper18

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.{FailureRateRestartStrategyConfiguration, FixedDelayRestartStrategyConfiguration, RestartStrategyConfiguration}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._


object FaultTolerance001 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //失败重试3次
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3,
      Time.of(10, TimeUnit.SECONDS)
    ))

    val ds1 = env.fromElements(2, 5, 3, 7, 9)
    ds1.map(_ * 2).print()
  }
}