package com.atguigu.chapter07

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/9/4 14:34
  */
object Flink03_CEP_API {
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
//      .assignAscendingTimestamps(_.ts * 1000L)
      .assignTimestampsAndWatermarks(
          new AssignerWithPunctuatedWatermarks[WaterSensor]{
            override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
              new Watermark(extractedTimestamp)
            }

            override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
              element.ts * 1000L
            }
          }
    )

    // TODO CEP的使用
    // 1.定义规则
    // times(n) 表示匹配n次，为 宽松近邻 的关系
    // times(m,n) 表示匹配  m 到 n 次都算，是 宽松近邻 的关系，比如 times(2,5)，表示 2次、3次、4次、5次都算
    // within(Time) 设置超时时间，大于等于 超时时间没匹配上，则匹配失败
    val pattern: Pattern[WaterSensor, WaterSensor] = Pattern
      .begin[WaterSensor]("start")
      .where(_.id == "sensor_1")
      .next("next")
      .where(_.id == "sensor_1")
//      .times(3)
//      .times(1,3)
      .within(Time.seconds(3))


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
