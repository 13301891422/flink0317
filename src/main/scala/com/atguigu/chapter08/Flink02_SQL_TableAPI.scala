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
object Flink02_SQL_TableAPI {
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
      //      .useBlinkPlanner()  // 使用Blink版本
      .inStreamingMode() // 基于流处理的模式
      //      .inBatchMode()  // 基于批处理的模式
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    // 2、把 DataStream转换成 Table对象
    val table: Table = tableEnv.fromDataStream(sensorDS,'id,'ts,'vc)

    // 3、使用 TableAPI对 Table进行操作
    val resultTable: Table = table
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 4、把 Table转换成 DataStream
    // 如果数据需要修改，那么 不能用 追加流 ，要用 回收流
    // true 表示 添加
    // false 表示 删除
    // 数据的更新过程： 将 原来的数据 标记为 false（删除），将更新后的数据 标记为 true（添加）
    resultTable.toRetractStream[Row].print("table api")

    env.execute()
  }
}
