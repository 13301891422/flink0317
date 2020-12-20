package com.atguigu.chapter08

import com.atguigu.chapter05.Flink22_Sink_MySQL.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.types.Row

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/5 10:04
  */
object Flink09_SQL_Window {
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

    // 2、把 DataStream转换成 Table对象
    // Table 指定时间语义： 字段.proctime => 处理时间      字段.rowtime => 指定事件时间
    val table1: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts.rowtime, 'vc)
    // 3、创建临时视图，给 Table对象 起一个 名字 （也可以给 DataStream起）
    tableEnv.createTemporaryView("sensorTable", table1)

    // 4、使用 SQL对 Table进行操作
   table1
     // 四步
     //     1、指定窗口的类型： 滚动、滑动
     //     2、指定窗口的参数： 窗口的长度
     //     3、指定设置时间语义的字段 => 在生成 Table的时候指定，通过 字段.xxxtime
     //     4、指定 窗口的别名
//       .window(Tumble over 5.minute on 'ts as 'w)
       .window(Tumble.over(5.minute) on 'ts as 'w) // 窗口类型是一个类，over是一个方法，窗口参数是 over方法的参数
     // 必须在 groupBy 中 传入 窗口别名
       .groupBy('id,'w)


    env.execute()
  }
}
