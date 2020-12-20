package com.atguigu.chapter06

import java.sql.Timestamp
import java.{lang, util}

import org.apache.flink.api.common.functions.AggregateFunction
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
  * 热门商品统计
  * 每5分钟 输出 最近一个小时内的 点击最多的前N个 商品
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink26_Case_HotItemAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
    // 转换成样例类
    val userBehaviorDS: DataStream[UserBehavior] = logDS
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

    // 3.处理数据:
    // 3.1 过滤
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")

/*    filterDS
      .map(data => (data.itemId, 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .sum(1)*/

    // 3.2 按照统计的维度进行分组：商品
    val userBehaviorKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)
    // 3.3 开窗
    val userBehaviorWS: WindowedStream[UserBehavior, Long, TimeWindow] = userBehaviorKS.timeWindow(Time.hours(1), Time.minutes(5))
    // 求和、排序、取前 N个
    // 怎么求和？
    // 怎么排序？
//    userBehaviorWS.sum(1) // 1、滚动聚合，每来一条都会输出一次结果； 2、没法排序
    //    userBehaviorWS.reduce()   // 1、滚动聚合  2、没法排序
    //    userBehaviorWS.process()  // 数据到齐之后再求和、排序，可以实现

    // 3.4 使用aggregate聚合
    //  第一个函数：预聚合函数，每来一条数据就累加一次，直到窗口触发的时候才会把 统计的结果值 传递给 全窗口函数
    //  第二个函数：全窗口函数，输入 就是 预聚合函数的结果，将数据打上 窗口结束时间 这个标记，用于后面 在流中 区分 来自不同窗口的数据
    val aggDS: DataStream[HotItemClick] = userBehaviorWS
      .aggregate(
        new MyAggregateFunction(),
        new MyProcessWindowFunction
      )

    // 3.5 按照 窗口结束时间 分组： 将同一个窗口的统计值，汇总到一个分组里，方便进行排序
    val aggKS: KeyedStream[HotItemClick, Long] = aggDS.keyBy(_.windowEnd)

    // 3.6 排序
    val processDS: DataStream[String] = aggKS.process(new MyKeyedProcessFunction())

    // 4. 打印
    processDS.print("hot item")


    // 5. 执行
    env.execute()
  }


  class MyKeyedProcessFunction extends KeyedProcessFunction[Long, HotItemClick, String] {

    private var dataList: ListState[HotItemClick] = _
    private var triggerTs: ValueState[Long] =_

    override def open(parameters: Configuration): Unit = {
      dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotItemClick]("data list", classOf[HotItemClick]))
      triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTs",classOf[Long]))
    }

    override def processElement(value: HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, out: Collector[String]): Unit = {
      // 排序？需要数据到齐，这个方法是一条一条处理，所以 =》 先存起来 =》 不同组分开存 =》 存到键控状态的 List 类型里
      dataList.add(value)
      // 什么时候算到齐了？存到啥时候？ 什么时候开始排序？ => 模拟窗口的触发，用定时器
      if (triggerTs.value() == 0) { // 防止重复注册定时器，避免重复创建对象
        ctx.timerService().registerEventTimeTimer(value.windowEnd)
        triggerTs.update(value.windowEnd)
      }
    }

    /**
      * 定时器触发，表示同一个窗口的数据到齐了，可以进行排序、取前N个
      *
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, out: Collector[String]): Unit = {
      //  获取保存的数据
      val dataIt: util.Iterator[HotItemClick] = dataList.get().iterator()
      //  将数据传递给scala的集合
      val listBuffer = new ListBuffer[HotItemClick]
      while (dataIt.hasNext) {
        listBuffer += dataIt.next()
      }
      // 清空保存的状态，因为已经没用了
      dataList.clear()
      triggerTs.clear()

      // 使用 scala的 ListBuffer中的 sort相关方法，进行排序，取前 N个
      // 数据条数 = 商品数量，大概是 十万级别 ， 数据量不大，不用考虑 oom的问题
      val top3: ListBuffer[HotItemClick] = listBuffer.sortWith(_.clickCount > _.clickCount).take(3)

      // 输出结果
      out.collect(
        s"""
           |窗口结束时间:${new Timestamp(timestamp)}
           |---------------------------------------------
           |Top3商品:${top3.mkString("\n")}
           |_____________________________________________
        """.stripMargin
      )
    }
  }


  /**
    * 预聚合函数
    */
  class MyAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }


  /**
    * 全窗口函数
    * 输入 就是 预聚合函数的输出
    */
  class MyProcessWindowFunction extends ProcessWindowFunction[Long, HotItemClick, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[HotItemClick]): Unit = {
      // 一次进入这个方法里的，只能是一个组的数据，否则，参数中的 key无法确定
      // 传入全窗口函数的数据，是统计的结果，每个商品只有一条，而每个商品都是一个分组，所以这里elements中只有一条数据：该商品的统计结果
      out.collect(HotItemClick(key, elements.iterator.next(), context.window.getEnd))
    }
  }

  /**
    * 全窗口输出的样例类
    *
    * @param itemId     商品ID
    * @param clickCount 窗口内的统计值
    * @param windowEnd  窗口的结束时间 - 用来给数据打上标签，用于区分是来源于哪个窗口
    */
  case class HotItemClick(itemId: Long, clickCount: Long, windowEnd: Long)

  /**
    * 用户行为日志样例类
    *
    * @param userId     用户ID
    * @param itemId     商品ID
    * @param categoryId 商品类目ID
    * @param behavior   用户行为类型
    * @param timestamp  时间戳（秒）
    */
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
