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
object Flink08_SQL_Operator {
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

    //
    val sensorDS2: DataStream[WaterSensor] = env
      .readTextFile("input/sensor-data-cep.log")
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

    // 2、把 DataStream转换成 Table对象
    val table1: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts, 'vc)
    val table2: Table = tableEnv.fromDataStream(sensorDS2, 'id, 'ts, 'vc)
    // 3、创建临时视图，给 Table对象 起一个 名字 （也可以给 DataStream起）
    tableEnv.createTemporaryView("sensorTable", table1)
    tableEnv.createTemporaryView("sensorTable2", table2)

    // 4、使用 SQL对 Table进行操作
    tableEnv
      //      .sqlQuery("select * from sensorTable")  // 查询
      //      .sqlQuery("select * from sensorTable where id = 'sensor_1' and vc > 50")  // 条件
      //      .sqlQuery("select id,count(id) as cnt from sensorTable group by id")  // 分组
            .sqlQuery(
              """
                |select *
                |from sensorTable s1
                |left join sensorTable2 s2 on s1.id = s2.id
              """.stripMargin)  // 连接:内、外
      /*      .sqlQuery(
              """
                |select
                |*
                |from
                |(select * from sensorTable where vc < 50)
                |union
                |(select * from sensorTable2)
              """.stripMargin)  // 连接：union - 去重*/
//      .sqlQuery(
//      """
//        |select
//        |*
//        |from sensorTable
//        |where id in
//        |(select id from sensorTable2)
//      """.stripMargin) // 范围： where ... in ...

      //      .toAppendStream[Row]
      .toRetractStream[Row]
      .print("sql query")


    env.execute()
  }
}
