package com.atguigu.chapter07

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/4 14:34
  */
object Flink02_CEP_API {
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
    // next 表示严格近邻，两个紧挨着，中间不能有第三者
    // followedBy 表示宽松近邻，两个不用紧挨着，中间可以有第三者、第四者。匹配上就不再匹配，类似 一夫一妻
    // followedByAny 表示 非确定性宽松近邻，跟宽松近邻的区别：匹配上一个，不会满足，还可以接着匹配。  类似  一夫多妻
    val pattern: Pattern[WaterSensor, WaterSensor] = Pattern
      .begin[WaterSensor]("start")
      .where(_.id == "sensor_1")
//      .next("next")
//      .followedBy("follow")
      .followedByAny("followedByAny")
      .where(_.id == "sensor_2")


    // 2.应用规则：把规则应用到 数据流中
    val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS, pattern)

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
