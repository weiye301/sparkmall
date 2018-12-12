package com.weiye.sparkmall.offline.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scala.collection.immutable.HashMap

class CityReamrkUDAF extends UserDefinedAggregateFunction {

  //定义输入的结构
  override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))

  //存放累计值
  //两个:一个是:map[city_name,count],另一个放当前区域当前商品的所有点击次数
  override def bufferSchema: StructType = StructType(Array(StructField("city_count", MapType(StringType, LongType)), StructField("total_count", LongType)))

  //定义输出值结构:打印结果的类型
  override def dataType: DataType = StringType

  //校验一致性:相同的输入有相同的输出
  override def deterministic: Boolean = true

  //初始化buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HashMap[String, Long]
    buffer(1) = 0L
  }

  //更新,每进入一条数据,进行累加
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val city_name: String = input.getString(0)
    if (city_name == null && city_name.isEmpty) {
      return
    }
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    val newCityCountMap: Map[String, Long] = cityCountMap + (city_name -> (cityCountMap.getOrElse(city_name, 0L) + 1L))
    buffer(0) = newCityCountMap
    buffer(1) = totalCount + 1L
  }

  //合并:把不同分区的结果进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)

    val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val totalCount2: Long = buffer2.getLong(1)

    val newCityCountMap: Map[String, Long] = cityCountMap1.foldLeft(cityCountMap2) { case (cityCountMap_2, (city_name, count)) =>
      cityCountMap_2 + (city_name -> (cityCountMap_2.getOrElse(city_name, 0L) + count))
    }

    buffer1(0) = newCityCountMap
    buffer1(1) = totalCount1 + totalCount2
  }

  //输出,展示结果
  override def evaluate(buffer: Row): Any = {
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    val cityCountInfoList: List[CityCountInfo] = cityCountMap.map { case (city_name, count) =>
      CityCountInfo(city_name, count, Math.round(count / totalCount.toDouble * 10000) / 100.0)
    }.toList
    val cityInfoTop2List: List[CityCountInfo] = cityCountInfoList.sortWith((cityInfo1, cityInfo2) => cityInfo1.cityCount > cityInfo2.cityCount).take(2)
    if (cityCountInfoList.size > 2) {
      var otherRatio: Double = 100.00
      cityInfoTop2List.foreach(cityInfo => otherRatio = Math.round((otherRatio - cityInfo.cityRatio) * 100) / 100.0)
      val withOtherInfoList: List[CityCountInfo] = cityInfoTop2List :+ CityCountInfo("其他", 0L, otherRatio)
      withOtherInfoList.mkString(",")
    } else {
      cityInfoTop2List.mkString(",")
    }
  }

  case class CityCountInfo(cityName: String, cityCount: Long, cityRatio: Double) {
    override def toString: String = cityName + ":" + cityRatio + "%"
  }

}
