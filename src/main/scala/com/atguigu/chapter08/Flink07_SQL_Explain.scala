package com.atguigu.chapter08

import com.atguigu.chapter05.Flink22_Sink_MySQL.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/5 10:04
  */
object Flink07_SQL_Explain {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val inputDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    val sensorDS: DataStream[WaterSensor] = inputDS
      .map(
        lines => {
          val datas: Array[String] = lines.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        }
      )
      .assignAscendingTimestamps(_.ts * 1000L)


    // TODO Table API
    // 1、 创建表的环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() // 使用官方的老版本
      .inStreamingMode() // 基于流处理的模式
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    // 2、把 DataStream转换成 Table对象
    val table: Table = tableEnv.fromDataStream(sensorDS,'id,'ts,'vc)

    // 3、使用 TableAPI对 Table进行操作
    val resultTable: Table = table
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 4、使用解释器 查看 结果表的 执行计划
    val exp: String = tableEnv.explain(resultTable)

    println(exp)

    env.execute()
  }
}
