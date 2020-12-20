package com.atguigu.chapter07

import java.sql.Timestamp

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 订单支付超时监控
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink06_Case_OrderTimeOut {
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
    // 局限性： 1. 没法发现，超时，但是支付的情况，但是一般这种情况，本身就是一种异常的情况
    //         2. 没法发现，create数据丢失的情况，但是这种情况，本身就是异常情况

    // 1.定义规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2.应用规则
    val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

    // 3.获取匹配规则的数据
    // select可以传 3 个参数：
    //    第一个参数：侧输出流的标签对象 => 侧输出流 用来放 超时数据 的处理结果
    //    第二个参数：对 超时数据的 处理 => 处理完的数据，放入侧输出流
    //    第三个参数：对 匹配上的数据 的处理
    //    三个参数可以通过 柯里化 的方式传参
    val timeoutTag = new OutputTag[String]("timeout")
    val resultDS: DataStream[String] = orderPS.select(timeoutTag)(
      (timeoutData, ts) => {
        timeoutData.toString()
      }
    )(
      data => {
        data.toString()
      }
    )

//    resultDS.print("result")
    resultDS.getSideOutput(timeoutTag).print("timeout")


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
