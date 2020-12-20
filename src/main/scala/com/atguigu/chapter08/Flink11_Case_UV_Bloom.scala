package com.atguigu.chapter08

import java.lang

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


/**
  * TODO
  *
  * @version 1.0
  * @author create by cjp on 2020/8/5 10:27
  */
object Flink11_Case_UV_Bloom {
  def main(args: Array[String]): Unit = {

    // 流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据，并封装成样例类
    val userBehaviorDS: DataStream[UserBehavior] = env
      .readTextFile("input/UserBehavior.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          UserBehavior(
            datas(0).toLong,
            datas(1).toLong,
            datas(2).toInt,
            datas(3),
            datas(4).toLong
          )
        }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 2.过滤出pv行为
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")

    // 3.转换数据格式只取userId，降低数据量
    val userDS: DataStream[Long] = filterDS.map(data => data.userId)

    // 4.开窗：1个小时长度，10秒滑动
    val timeAS: AllWindowedStream[Long, TimeWindow] = userDS
      .timeWindowAll(Time.hours(1), Time.seconds(10))
      .trigger(
        new Trigger[Long, TimeWindow] {
          // 来一条数据，怎么处理？
          override def onElement(element: Long, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
            TriggerResult.FIRE_AND_PURGE
          }

          // 到达处理时间，怎么处理？ => 不处理，跳过
          override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
            TriggerResult.CONTINUE
          }

          // 到达事件时间，怎么处理？ => 不处理，跳过
          override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
            TriggerResult.CONTINUE
          }

          // 清除
          override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
        }
      )

    // 5.利用布隆过滤器作去重处理
    timeAS.process(
      new ProcessAllWindowFunction[Long, String, TimeWindow] {

        private var jedis: Jedis = _

        override def open(parameters: Configuration): Unit = {
          jedis = new Jedis("hadoop102", 6379)
        }

        override def process(context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
          val bloomFilter = new MyBloom()
          val userId: String = elements.iterator.next().toString
          val offset: Long = bloomFilter.offset(userId, 61)
          val windowEnd: String = context.window.getEnd.toString
          val isExist: lang.Boolean = jedis.getbit(windowEnd, offset)

          // 如果位图中存在，表示用户已经统计过，不做操作

          // 如果位图中不存在，表示用户还没来过 => 1.更新redis中的uv值，+1 2.更新位图为true
          if (!isExist) {
            // 更新redis中的uv值，先查询一下当前值
            var uvCount: String = jedis.hget("uvCount", windowEnd)
            if (uvCount == null || "".equals(uvCount)) {
              // 如果当前是该窗口的第一条数据，给个初始值
              uvCount = "1"
            } else {
              uvCount = (uvCount.toLong + 1).toString
            }
            // 更新到redis中
            jedis.hset("uvCount", windowEnd, uvCount)

            // 更新位图
            jedis.setbit(windowEnd, offset, true)
          }

          out.collect("数据=" + elements)
        }
      }
    )
      .print()


    env.execute()

  }


  /**
    * 用户行为数据样例类
    *
    * @param userId     用户ID
    * @param itemId     商品ID
    * @param categoryId 商品类目ID
    * @param behavior   用户的行为：点击、购买、收藏、喜欢
    * @param timestamp  时间戳
    */
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


  class MyBloom() {
    val cap = 1 << 30
    var hash = 0L

    /**
      * 计算用户ID和位图中offset的映射关系
      * 也就是说，给一个用户ID，算出一个唯一的offset
      *
      */
    def offset(s: String, seed: Int): Long = {

      for (c <- s) {
        hash = hash * seed + c
      }
      //参考hashmap计算key的hash值的算法
      hash & (cap - 1)
    }
  }

}
