package com.atguigu.chapter05

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink_CheckPoint_Test {
      def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(20000) //1分钟做一次checkpoint
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //仅仅一次
    checkpointConfig.setMinPauseBetweenCheckpoints(30000l) //设置checkpoint间隔时间30秒
    checkpointConfig.setCheckpointTimeout(10000l) //设置checkpoint超时时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //cancel时保留checkpoint
    //设置statebackend 为rockdb
    //    val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint")
    //    env.setStateBackend(stateBackend)

    env.setStateBackend(new RocksDBStateBackend("hdfs:///C:\\bigData\\ideaWorkSpace\\flink-scala-code\\output"))



    //设置重启策略   重启3次 间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))

    //kafka的参数设置
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop105:9092")
    props.setProperty("group.id", "test-consumer-group")

    import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
    val kafkaDStream = env.addSource(new FlinkKafkaConsumer010[String]
    ("memberpaymoney", new SimpleStringSchema(), props))

    // 转换成样例类
    val payMoneyDS: DataStream[DwdMemberPayMoney] = kafkaDStream.map(
      line => {
        import com.alibaba.fastjson.JSONObject
        val jsonObj = JSON.parseObject(line)
        DwdMemberPayMoney(
          jsonObj.getIntValue("uid"),
          jsonObj.getDouble("paymoney"),
          jsonObj.getIntValue("siteid"),
          jsonObj.getIntValue("vip_id"),
          jsonObj.getString("createtime"),
          jsonObj.getString("dt"),
          jsonObj.getString("dn"))
      }
    )

//    payMoneyDS.print()
    // 3.处理数据：参照wordcount实现的思路
    // 3.1 能过滤，先过滤 => 过滤出需要的数据，减少数据量，优化效率
    val filterDS: DataStream[DwdMemberPayMoney] = payMoneyDS.filter(_.dt == "20201212")
    //    // 3.2 转换成（ pv，1）
//    val payMoneyMapDS: DataStream[(Int, Double)] = filterDS.map(payMoney => (payMoney.vip_id, payMoney.paymoney))

    val keyDS: KeyedStream[DwdMemberPayMoney, Int] = filterDS.keyBy(_.vip_id)
    keyDS.process(
      new KeyedProcessFunction[Int,DwdMemberPayMoney,String] {
        private var sumState: ValueState[Double] = _

        override def open(parameters: Configuration): Unit = {
          sumState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("sum", classOf[Double]))
        }

        override def processElement(value: DwdMemberPayMoney, ctx: KeyedProcessFunction[Int, DwdMemberPayMoney, String]
          #Context, out: Collector[String]): Unit = {
          var currentSum: Double = sumState.value()
          currentSum += value.paymoney
          sumState.update(currentSum)
          //            println("当前key=" + ctx.getCurrentKey + value.toString+",count值 = "+count)
          println("当前vip等级= " + ctx.getCurrentKey  + ",sum值 = " + sumState.value() + ",当前时间为:"+value.createtime)        }
      }
    ).print()



    // 5. 执行
    env.execute()
  }


//  /**
//    * 用户行为日志样例类
//    *
//    * @param userId     用户ID
//    * @param itemId     商品ID
//    * @param categoryId 商品类目ID
//    * @param behavior   用户行为类型
//    * @param timestamp  时间戳（秒）
//    */
//  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  case class DwdMemberPayMoney
  (uid: Int, var paymoney: Double, siteid: Int, vip_id: Int, createtime: String, dt: String, dn: String)

}
