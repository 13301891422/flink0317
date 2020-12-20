package com.atguigu.chapter06

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 订单支付超时监控
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink31_Case_OrderTimeOut {
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
    // 3.2
    val resultDS: DataStream[String] = orderKS.process(
      new KeyedProcessFunction[Long, OrderEvent, String] {

        private var payEvent: ValueState[OrderEvent] = _
        private var createEvent: ValueState[OrderEvent] = _
        private var timeoutTs: ValueState[Long] = _

        val timeoutTag = new OutputTag[String]("timeout")

        override def open(parameters: Configuration): Unit = {
          payEvent = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payEvent", classOf[OrderEvent]))
          createEvent = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("createEvent", classOf[OrderEvent]))
          timeoutTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeoutTs", classOf[Long]))
        }

        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = {

          // 每个订单的第一条数据来的时候，应该注册一个定时器
          if (timeoutTs.value() == 0) {
            // 不管来的是 create还是pay，没必要作区分，直接等15分钟，不来，要么异常，要么超时
            // 定时器的作用，就是发现只有一个数据来的情况
//            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
            timeoutTs.update(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
          } else {
            // 表示来的不是本订单的第一条数据，表示create和pay都到了，那么可以删除定时器，后面的逻辑有对超时进行判断
//            ctx.timerService().deleteProcessingTimeTimer(timeoutTs.value())
            ctx.timerService().deleteEventTimeTimer(timeoutTs.value())
            timeoutTs.clear()
          }


          // 来的数据可能是create，也可能是pay
          if (value.eventType == "create") {
            // 1.来的数据是create
            // 判断 pay来过没有
            if (payEvent.value() == null) {
              // 1.1 pay没来过 => 把 create保存起来，等待pay的来临
              createEvent.update(value)
            } else {
              // 1.2 pay来过,判断一下是否超时
              if (payEvent.value().eventTime - value.eventTime > 15 * 60) {
                val timeoutTag = new OutputTag[String]("timeout")
                ctx.output(timeoutTag, "订单" + value.orderId + "支付成功，但是超时，请检查业务系统是否存在异常！")
              } else {
                out.collect("订单" + value.orderId + "支付成功！")
              }
              payEvent.clear()
            }
          } else {
            // 2.来的数据是pay => 判断 create 来过没有
            if (createEvent.value() == null) {
              // 2.1 create没来过 => 把 pay保存起来
              payEvent.update(value)
            } else {
              // 2.2 create已经来了
              if (value.eventTime - createEvent.value().eventTime > 15 * 60) {

                ctx.output(timeoutTag, "订单" + value.orderId + "支付成功，但是超时，请检查业务系统是否存在异常！")
              } else {
                out.collect("订单" + value.orderId + "支付成功！")
              }
              createEvent.clear()
            }

          }

        }

        /**
          * 触发操作：说明另一条数据没来
          * @param timestamp
          * @param ctx
          * @param out
          */
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
          // 判断是谁没来
          if (payEvent.value() != null){
            // 1. pay来过，create没来
            ctx.output(timeoutTag,"订单"+payEvent.value().orderId + "有支付数据，但 create数据丢失，数据异常！！！")
            payEvent.clear()
          }
          if (createEvent.value() !=null){
            // 2. create来过，pay没来
            ctx.output(timeoutTag,"订单"+createEvent.value().orderId + "超时，用户未支付！")
            createEvent.clear()
          }

          // 清空保存的定时时间
          timeoutTs.clear()
        }
      }
    )

    val timeoutTag = new OutputTag[String]("timeout")
    resultDS.getSideOutput(timeoutTag).print("timeout")
    resultDS.print("result")




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
