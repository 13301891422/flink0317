package com.atguigu.chapter07

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 订单支付超时监控
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink05_Case_OrderTimeOut {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val orderDS: DataStream[OrderEvent] = env
      .readTextFile("input/OrderLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          OrderEvent(
            datas(0).toLong,
            datas(1),
            datas(2),
            datas(3).toLong
          )
        }
      )
      .assignAscendingTimestamps(_.eventTime * 1000L)

    // 3. 处理数据
    // 3.1 按照统计的维度分组：订单
    val orderKS: KeyedStream[OrderEvent, Long] = orderDS.keyBy(_.orderId)
    // 3.2 使用CEP实现超时监控

    // 1.定义规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2.应用规则
    val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS,pattern)

    // 3.获取匹配规则的数据
    orderPS.select(
      data => {
        val createEvent: OrderEvent = data("create").iterator.next()
        val payEvent: OrderEvent = data("pay").iterator.next()
        s"""
          |订单ID:${createEvent.orderId}
          |创建时间:${new Timestamp(createEvent.eventTime)}
          |支付时间:${new Timestamp(payEvent.eventTime)}
          |耗时:${payEvent.eventTime - createEvent.eventTime}秒
          |===========================================================
        """.stripMargin
      }
    )
        .print("result")




    // 5. 执行
    env.execute()
  }

  /**
    * 订单数据样例类
    *
    * @param orderId   订单ID
    * @param eventType 时间类型：创建、支付
    * @param txId      交易码
    * @param eventTime 事件时间
    */
  case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

}
