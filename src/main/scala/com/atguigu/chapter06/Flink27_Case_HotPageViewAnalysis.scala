package com.atguigu.chapter06

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.atguigu.function.SimplePreAggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 热门页面浏览量统计
  * 每隔 5秒，输出最近 10分钟内访问量最多的前N个URL
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink27_Case_HotPageViewAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val logDS: DataStream[String] = env.readTextFile("input/apache.log")
    // 转换成样例类
    val apacheLogDS: DataStream[ApacheLog] = logDS
      .map(
        line => {
          val datas: Array[String] = line.split(" ")
          val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val ts: Long = sdf.parse(datas(3)).getTime
          ApacheLog(
            datas(0),
            datas(1),
            ts,
            datas(5),
            datas(6)
          )
        }
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
          override def extractTimestamp(element: ApacheLog): Long = element.eventTime
        }
      )

    // 3.处理数据:
    // 3.1 按照 统计维度：URL 分组
//    apacheLogDS.filter()
    val apacheLogKS: KeyedStream[ApacheLog, String] = apacheLogDS.keyBy(_.url)
    // 3.2 开窗：每 5秒，输出最近 10分钟内
    val apacheLogWS: WindowedStream[ApacheLog, String, TimeWindow] = apacheLogKS.timeWindow(Time.minutes(10), Time.seconds(5))
    // 3.3  聚合操作：
    //  第一个函数：作 预聚合操作，结果传递给全窗口函数
    //  第二个函数：将预聚合的结果，加上 窗口结束时间 的标记，方便后面 按照窗口进行分组
    val aggDS: DataStream[HotPageView] = apacheLogWS.aggregate(
      new SimplePreAggregateFunction[ApacheLog](),
      new MyProcessWindowFunction()
    )

    // 3.4 按照 窗口结束时间 分组：同来自于 同一个窗口的数据，放在一起，方便进行排序
    val aggKS: KeyedStream[HotPageView, Long] = aggDS.keyBy(_.windowEnd)
    // 3.5 进行排序
    val resultDS: DataStream[String] = aggKS.process(
      new KeyedProcessFunction[Long, HotPageView, String] {

        private var dataList: ListState[HotPageView] = _
        private var triggerTs: ValueState[Long] = _


        override def open(parameters: Configuration): Unit = {
          dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotPageView]("dataList", classOf[HotPageView]))
          triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTs", classOf[Long]))
        }

        override def processElement(value: HotPageView, ctx: KeyedProcessFunction[Long, HotPageView, String]#Context, out: Collector[String]): Unit = {
          // 1.存数据
          dataList.add(value)
          // 2.用定时器，模拟窗口的触发操作
          if (triggerTs.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEnd)
            triggerTs.update(value.windowEnd)
          }
        }

        /**
          * 定时器触发的操作：排序 =》取前N
          *
          * @param timestamp
          * @param ctx
          * @param out
          */
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotPageView, String]#OnTimerContext, out: Collector[String]): Unit = {
          // 1.从集合取出
          val datas: util.Iterator[HotPageView] = dataList.get().iterator()
          // 2.放入scala的集合
          val listBuffer = new ListBuffer[HotPageView]
          while (datas.hasNext) {
            listBuffer.append(datas.next())
          }
          // 3.排序,取前 3
          val top3: ListBuffer[HotPageView] = listBuffer.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

          // 4.通过采集器发送到下游
          out.collect(
            s"""
窗口结束时间:${new Timestamp(timestamp)}
------------------------------------------
 ${top3.mkString("\n")}
__________________________________________
            """.stripMargin
          )
        }
      }
    )

    // 4. 打印
    resultDS.print("top3 hot page")


    // 5. 执行
    env.execute()
  }

  /**
    * 全窗口函数：给 每个窗口的统计结果，加上 窗口结束时间 的标记
    * 输入 是 预聚合函数的输出，也就是Long
    */
  class MyProcessWindowFunction extends ProcessWindowFunction[Long, HotPageView, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[HotPageView]): Unit = {
      out.collect(HotPageView(key, elements.iterator.next(), context.window.getEnd))
    }
  }

  case class HotPageView(url: String, clickCount: Long, windowEnd: Long)

  /**
    * 页面浏览的样例类
    *
    * @param ip        用户IP
    * @param userId    用户ID
    * @param eventTime 页面访问的事件
    * @param method    请求的方法：GET、POST
    * @param url       访问的页面
    */
  case class ApacheLog(
                        ip: String,
                        userId: String,
                        eventTime: Long,
                        method: String,
                        url: String)

}
