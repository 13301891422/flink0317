package com.atguigu.chapter08

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.types.Row

/**
  * 热门商品统计
  * 每5分钟 输出 最近一个小时内的 点击最多的前N个 商品
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink10_SQL_HotItemAnalysis {
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

    // 3. 过滤
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")

    //TODO Table API & SQL 实现 TopN 热门商品统计
    // 4. 获取表环境:Blink
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 5. 将 DataStream转换成Table
    val table: Table = tableEnv.fromDataStream(filterDS, 'itemId, 'timestamp.rowtime as 'ts)

    // 6. 使用TableAPI：开滑动窗口、按照维度（商品）分组、聚合、打上windowEnd
    val aggDS: DataStream[Row] = table
      .window(Slide over 1.hours every 5.minutes on 'ts as 'w)
      .groupBy('itemId, 'w)
      .aggregate('itemId.count as 'itemCount)
      .select('itemId, 'itemCount, 'w.end as 'windowEnd)
      .toAppendStream[Row]

    tableEnv.createTemporaryView("aggTable", aggDS,'itemId,'itemCount,'windowEnd)
    // 经过 6之后，数据内容如下： 商品ID、统计值、窗口结束时间
    //（1001,10000,windowEnd1）
    //（1002,500,windowEnd1）
    //（1003,5000,windowEnd1）
    //（1004,50,windowEnd1）
    //（1001,15000,windowEnd2）
    //（1002,1500,windowEnd2）
    //（1003,5600,windowEnd2）
    //（1004,90,windowEnd2）


    // 7. 使用SQL实现TopN
    tableEnv
      .sqlQuery(
        """
          |select
          |*
          |from (
          |   select
          |   *,
          |   row_number() over(
          |     partition by windowEnd
          |     order by itemCount desc ) ranknum
          |   from aggTable
          |) t1
          |where ranknum <= 3
        """.stripMargin)
      .toRetractStream[Row]
      .print()

    // 5. 执行
    env.execute()
  }

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
