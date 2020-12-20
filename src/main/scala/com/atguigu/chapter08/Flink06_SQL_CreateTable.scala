package com.atguigu.chapter08

import com.atguigu.chapter05.Flink22_Sink_MySQL.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/5 10:04
  */
object Flink06_SQL_CreateTable {
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


    // TODO SQL
    // 1、 创建表的环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() // 使用官方的老版本
      .inStreamingMode() // 基于流处理的模式
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 将外部系统抽象成一个Table，给定表名
    tableEnv
      .connect(new FileSystem().path("output/flink.txt"))
      .withFormat(new OldCsv())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("vc",DataTypes.INT())
      )
      .createTemporaryTable("fsTable")

    // 将注册好的 Table，通过表名，实例化成 Table对象
    tableEnv.createTemporaryView("sensorTable",sensorDS)
    val table: Table = tableEnv.from("sensorTable")

    // 把 DataStream转换成 Table对象
//    val table: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts, 'vc)
    // 创建临时视图，给 Table对象 起一个 名字 （也可以给 DataStream起）
//    tableEnv.createTemporaryView("sensorTable",table)

    // 4、使用 SQL对 Table进行操作
    tableEnv
      .sqlQuery("select * from sensorTable")
      .toAppendStream[Row]
      .print("sql query")


    env.execute()
  }
}
