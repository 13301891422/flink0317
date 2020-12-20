package com.atguigu.chapter06

import java.sql.Timestamp
import java.util

import com.atguigu.function.SimplePreAggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 统计 各个省份 的 每个广告 的点击量
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink28_Case_AdClickAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val adClickDS: DataStream[AdClickLog] = env
      .readTextFile("input/AdClickLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          AdClickLog(
            datas(0).toLong,
            datas(1).toLong,
            datas(2),
            datas(3),
            datas(4).toLong
          )
        }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3.处理数据
    // 3.1 按照统计的维度进行分组（维度：省份、广告），多个维度可以拼接在一起
    val provinceAndAdAndOneKS: KeyedStream[AdClickLog, (String, Long)] = adClickDS
      //      .map(adClick => (adClick.province + "_" + adClick.adId, 1L))
//      .keyBy(data => (data.province, data.adId))
      .keyBy(data => (data.province, data.adId))

    // 3.2 按照分组进行求和统计
    provinceAndAdAndOneKS
//      .timeWindow(Time.hours(1), Time.minutes(5))
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(
        new SimplePreAggregateFunction[AdClickLog](),
        new ProcessWindowFunction[Long, HotAdClick, (String, Long), TimeWindow] {
          override def process(key: (String, Long), context: Context, elements: Iterable[Long], out: Collector[HotAdClick]): Unit = {
            out.collect(HotAdClick(key._1, key._2, elements.iterator.next(), context.window.getEnd))
          }
        }
      )
      .keyBy(_.windowEnd)
      .process(
        new KeyedProcessFunction[Long, HotAdClick, String] {
          private var dataList: ListState[HotAdClick] = _
          private var triggerTs: ValueState[Long] = _


          override def open(parameters: Configuration): Unit = {
            dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotAdClick]("dataList", classOf[HotAdClick]))
            triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTs", classOf[Long]))
          }

          override def processElement(value: HotAdClick, ctx: KeyedProcessFunction[Long, HotAdClick, String]#Context, out: Collector[String]): Unit = {
            // 1.存
            dataList.add(value)
            // 2.用定时器，模拟窗口的触发
            if (triggerTs.value() == 0) {
              ctx.timerService().registerEventTimeTimer(value.windowEnd)
              triggerTs.update(value.windowEnd)
            }
          }

          /**
            * 定时器触发：排序、 取前 N
            *
            * @param timestamp
            * @param ctx
            * @param out
            */
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotAdClick, String]#OnTimerContext, out: Collector[String]): Unit = {
            val datas: util.Iterator[HotAdClick] = dataList.get().iterator()
            val listBuffer = new ListBuffer[HotAdClick]
            while (datas.hasNext) {
              listBuffer.append(datas.next())
            }
            dataList.clear()
            triggerTs.clear()

            val top3: ListBuffer[HotAdClick] = listBuffer.sortWith(_.clickCount > _.clickCount).take(3)

            out.collect(
              s"""
                 |窗口结束时间:${new Timestamp(timestamp)}
                 |==============================================
                 | ${top3.mkString("\n")}
                 |==============================================
              """.stripMargin
            )
          }
        }
      )
      .print("top3 ad click")



    // 5. 执行
    env.execute()
  }


  case class HotAdClick(province: String, adId: Long, clickCount: Long, windowEnd: Long)

  /**
    * 广告点击样例类
    *
    * @param userId    用户ID
    * @param adId      广告ID
    * @param province  省份
    * @param city      城市
    * @param timestamp 时间戳
    */
  case class AdClickLog(
                         userId: Long,
                         adId: Long,
                         province: String,
                         city: String,
                         timestamp: Long)


}
