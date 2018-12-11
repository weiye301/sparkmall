package com.weiye.sparkmall.offline.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 定义累加器用于累加符合条件的session个数
  */
class CategoryActionCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var sessionMap = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = sessionMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryActionCountAccumulator
    accumulator.sessionMap ++= sessionMap
    accumulator
  }

  override def reset(): Unit = {
    sessionMap = new mutable.HashMap[String, Long]()
  }

  override def add(v: String): Unit = {
    sessionMap(v) = sessionMap.getOrElse(v, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    sessionMap = sessionMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count
      otherMap
    }
  }

  override def value: mutable.HashMap[String, Long] = sessionMap
}
