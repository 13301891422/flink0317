package com.atguigu.chapter08

import com.atguigu.chapter05.Flink22_Sink_MySQL.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/5 10:04
  */
object Flink01_SQL_TableAPI {
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
    // 指定字段的细节：
    //    基于位置：如果 列名 在DataStream的数据类型里 找不到，那么就用 位置 匹配
    //    基于名称： 如果 列名 在 DS里的数据类型能 找到，那么可以随以调换位置，会根据名称匹配
    val table: Table = tableEnv.fromDataStream(sensorDS,'bb,'aa)

    // 3、使用 TableAPI对 Table进行操作
    // 字段名的引用：前面加一个单引号
    // 判断相等： 用 三个 = 号， ===
    val resultTable: Table = table
//      .filter("id == 'sensor_2'")
      .filter('id === "sensor_2")
      .select('id,'vc,'ts)
//      .where()

    // 4、把 Table转换成 DataStream
//    resultTable.toAppendStream[WaterSensor].print("table api")
//    resultTable.toAppendStream[(String,Int)].print("table api")
    resultTable.toAppendStream[Row].print("table api")

    env.execute()
  }
}
