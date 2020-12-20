package com.atguigu.chapter08

import com.atguigu.chapter05.Flink22_Sink_MySQL.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/5 10:04
  */
object Flink05_SQL_Connect {
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
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 2、把 DataStream转换成 Table对象
    val table: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts, 'vc)
    tableEnv.createTemporaryView("sensorTable",table)

    // 3、将结果表，保存到外部系统：文件
    // TODO 将外部系统 抽象成 一张 Table,这样就变成 Table 与 Table之间的操作了
    // connect  连接外部系统：文件、kafka、ES
    // withFormat 指定 数据存储的 格式。 类比 Hive 在 HDFS上存储的 数据文件里 的 数据格式，指定分隔符等
    // withSchema 指定表结构 => 表的 字段名 和 字段类型
    // createTemporaryTable 指定表名
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

    // 4、使用SQL向外部系统插入数据
    tableEnv
        .sqlUpdate(
          """
            |INSERT INTO fsTable
            |SELECT *
            |FROM sensorTable
          """.stripMargin)

    env.execute()
  }
}
