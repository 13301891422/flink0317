package com.atguigu.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 简单的预聚合函数
  *
  * @version 1.0
  * @author create by cjp on 2020/9/2 16:13
  */
class SimplePreAggregateFunction[T] extends AggregateFunction[T, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: T, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
