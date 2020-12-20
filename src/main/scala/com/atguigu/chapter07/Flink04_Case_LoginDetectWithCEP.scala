package com.atguigu.chapter07

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 恶意登陆检测 - 2s 内连续 2次登陆失败
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink04_Case_LoginDetectWithCEP {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val loginDS: DataStream[LoginEvent] = env
      .readTextFile("input/LoginLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          LoginEvent(
            datas(0).toLong,
            datas(1),
            datas(2),
            datas(3).toLong
          )
        }
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
          override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
        }
      )

    // 3. 处理数据
    //    1、如果2s内多次失败呢？ 10次？ 用ListState？
    //    2、代码中并没有保证两次失败是连续的？ 中间隔了成功呢？
    //    3、数据是乱序的， 应该是 F、S、F，但是数据来的顺序是 F、F、S，那么会不会误判？

    // 3.1 按照统计维度分组：用户
    val loginKS: KeyedStream[LoginEvent, Long] = loginDS.keyBy(_.userId)

    // 3.2 使用CEP实现

    // 1.定义规则
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("start").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 2.应用规则
    val loginPS: PatternStream[LoginEvent] = CEP.pattern(loginKS,pattern)

    // 3.获取匹配规则的结果
    val resultDS: DataStream[String] = loginPS.select(
      data => {
        data.toString()
      }
    )


    resultDS.print("login detect with cep")




    // 5. 执行
    env.execute()
  }


  /**
    * 登陆行为样例类
    *
    * @param userId    用户ID
    * @param ip
    * @param eventType 事件类型：成功、失败
    * @param eventTime 时间时间
    */
  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)


}
