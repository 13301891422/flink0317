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
object Flink29_Case_BlackListFilter {
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
    val provinceAndAdAndOneKS: KeyedStream[AdClickLog, (Long, Long)] = adClickDS
      .keyBy(data => (data.userId, data.adId))

    // 3.2  进行黑名单过滤
    val blackFilterDS: DataStream[AdClickLog] = provinceAndAdAndOneKS.process(new BlackListFilter())
    val alarmTag = new OutputTag[String]("alarm")
    blackFilterDS.getSideOutput(alarmTag).print("blacklist")

    // 3.3 正常的业务处理逻辑，比如 topN
    blackFilterDS
      .keyBy(data => (data.userId, data.adId))
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(
        new SimplePreAggregateFunction[AdClickLog](),
        new ProcessWindowFunction[Long, HotAdClick, (Long, Long), TimeWindow] {
          override def process(key: (Long, Long), context: Context, elements: Iterable[Long], out: Collector[HotAdClick]): Unit = {
            out.collect(HotAdClick(key._1, key._2, elements.iterator.next(), context.window.getEnd))
          }
        }
      )
      .keyBy(_.windowEnd)
      .process(
        new KeyedProcessFunction[Long, HotAdClick, String] {
          override def processElement(value: HotAdClick, ctx: KeyedProcessFunction[Long, HotAdClick, String]#Context, out: Collector[String]): Unit = {
            out.collect("用户" + value.userId + "对广告" + value.adId + "点击了" + value.clickCount + "次")
          }
        }
      )
      .print("result")



    // 5. 执行
    env.execute()
  }

  class BlackListFilter extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
    // 定义一个 值状态，用来保存每个用户对某个广告的点击数
    private var clickCount: ValueState[Int] = _
    private var timerTs: Long = 0L
    private var alarmFlag: ValueState[Boolean] = _


    override def open(parameters: Configuration): Unit = {
      clickCount = getRuntimeContext.getState(new ValueStateDescriptor[Int]("clickCount", classOf[Int]))
      alarmFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("alarmFlag", classOf[Boolean]))
    }

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
      var currentCount: Int = clickCount.value()

      // 如果点击量 = 0，说明是今天第一次点击，也就是说，这条数据属于今天
      if (currentCount == 0) {
        // 考虑隔天 0点，对 count值 清零 => 用定时器来实现 => 需要获取隔天 0点的时间戳
        // 1、取整 => 取当天0点的时间戳 对应的 天数      2020-09-04 10：18：50 =>  2020-09-04 00:00:00 对应的天数
        val currentDay: Long = value.timestamp * 1000L / (24 * 60 * 60 * 1000L)
        // 2、隔天的天数 = 当前天数+1                   2020-09-04 00:00:00 对应的天数+1 => 2020-09-05 00:00:00 对应的天数
        val nextDay: Long = currentDay + 1
        // 3、隔天天数 转成 时间戳                      2020-09-05 00:00:00 对应的时间戳
        val nextDayStartTs: Long = nextDay * (24 * 60 * 60 * 1000L)

        // 注册隔天 0 点的定时器，做了判断，防止不同用户的数据都来注册定时器，如果日活很大，那么重复创建的次数也很多
        if (timerTs == 0) {
          //          ctx.timerService().registerProcessingTimeTimer(nextDayStartTs)
          ctx.timerService().registerEventTimeTimer(nextDayStartTs)
          timerTs = nextDayStartTs
        }
      }

      if (currentCount >= 100) {
        // 如果点击量超过阈值100，那么就 侧输出流 写入告警信息
        // 如果还没告警，就告警； 告警过了之后，不再告警
        if (!alarmFlag.value()) {
          // boolean默认值是false，那么进入到这里，说明还没告警
          // 首次告警，正常输出到侧输出流
          val alarmTag = new OutputTag[String]("alarm")
          ctx.output(alarmTag, "用户" + value.userId + "对广告" + value.adId + "今日内点击达到阈值，可能为恶意刷单")
          alarmFlag.update(true)
        }
      } else {
        // 没达到阈值，那么就正常统计点击量，并且往下游传递数据
        clickCount.update(currentCount + 1)
        //        out.collect("用户" + value.userId + "对广告" + value.adId + "点击了" + clickCount.value() + "次")
        out.collect(value)
      }

    }

    /**
      * 定时器触发：说明已经到了隔天 0点，要清空 count值
      *
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
      clickCount.clear()
      timerTs = 0L
      alarmFlag.clear()
    }
  }


  //  case class HotAdClick(province: String, adId: Long, clickCount: Long, windowEnd: Long)
  case class HotAdClick(userId: Long, adId: Long, clickCount: Long, windowEnd: Long)

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
