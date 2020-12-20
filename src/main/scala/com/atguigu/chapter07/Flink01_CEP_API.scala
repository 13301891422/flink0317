package com.atguigu.chapter07

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/4 14:34
  */
object Flink01_CEP_API {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorDS: DataStream[WaterSensor] = env
      .readTextFile("input/sensor-data-cep.log")
      .map(
        data => {
          val datas: Array[String] = data.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        }
      )
      .assignAscendingTimestamps(_.ts * 1000L)

    // TODO CEP的使用
    // 1.定义规则
    // 函数链
    // where 表示指定条件，多个where之间，是 且、and 的关系
    // or 表示 或 ，可以使用多个 or
    val pattern: Pattern[WaterSensor, WaterSensor] = Pattern
      .begin[WaterSensor]("start")
      .where(_.id == "sensor_1")
//      .where(_.vc > 5)
//      .or(_.vc > 5)



    // 2.应用规则：把规则应用到 数据流中
    //    第一个参数：要匹配规则的数据流
    //    第二个参数：定义的规则
    val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS,pattern)

    // 3.获取匹配的结果
    val resultDS: DataStream[String] = sensorPS.select(
      data => {
        data.toString()
      }
    )


    resultDS.print("cep")

    env.execute()

  }
}
