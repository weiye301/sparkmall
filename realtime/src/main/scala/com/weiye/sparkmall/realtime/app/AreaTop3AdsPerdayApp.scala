package com.weiye.sparkmall.realtime.app

import com.weiye.sparkmall.common.util.MyRedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

object AreaTop3AdsPerdayApp {

  def calcTop3Ads(adsCountSumDStream: DStream[(String, Long)]): Unit = {
    //    RDD[day_area_city_adsId,count]

    //    RDD[(day_area_adsId,count)]=>不同城市但是地区一样的进行聚合（reducebykey）
    //    => RDD[(day_area_adsId,count)]
    val areaAdsDayCount: DStream[(String, Long)] = adsCountSumDStream.map { case (areaCityAdsDay, count) =>
      //把字段切分,去掉城市,组成新的字段做key
      val fields: Array[String] = areaCityAdsDay.split(":")
      val area: String = fields(0)
      val adsId: String = fields(2)
      val day: String = fields(3)
      val key: String = area + ":" + adsId + ":" + day
      (key, count)
    }.reduceByKey(_ + _)

    //    => RDD[(day,(area,(adsId,count)))]  =>groupbykey
    val dayIterAreaDS: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsDayCount.map { case (areaAdsDay, count) =>
      val fields: Array[String] = areaAdsDay.split(":")
      val area: String = fields(0)
      val ads: String = fields(1)
      val day: String = fields(2)
      (day, (area, (ads, count)))
    }.groupByKey()

    //=> RDD[(day,iterable(area,(adsId,count)))]  要聚合成以area为key的广告计数集合
    val areaTop3AdsCountDS: DStream[(String, Map[String, String])] = dayIterAreaDS.map { case (day, iterArea) =>
      //    =>  iterable(area,(adsId,count))->groupby  ->  Map[area,iterable[area,(adsId,count)]]
      //将iterArea以地区分组
      val areaAdsCountMap: Map[String, Iterable[(String, (String, Long))]] = iterArea.groupBy { case (area, (ads, count)) => area }

      // iterable[area,(adsId,count)]-> iterable[(adsId,count)]->sort ->take(3)-> Json
      val areaTop3AdsCountJsonMap: Map[String, String] = areaAdsCountMap.map { case (area, iterAreaAdsCount) =>

        //因为iterAreaAdsCount里还有一个area,多余了所以去掉多余的
        val adsCount: Iterable[(String, Long)] = iterAreaAdsCount.map { case (area, (ads, count)) => (ads, count) }

        //去掉多余属性后根据广告点击次数排序取前三个
        val top3AdsCount: List[(String, Long)] = adsCount.toList.sortWith((adsCount1, adsCount2) => adsCount1._2 > adsCount2._2).take(3)

        //redis中不能存list所以需要把List转换成json字符串
        val top3AdsCountJson: String = compact(render(top3AdsCount))

        //返回(地区,json字符串)作为 redis 最外层 天数 的属性
        (area, top3AdsCountJson)
      }
      (day, areaTop3AdsCountJsonMap)
    }

    //存redis
    areaTop3AdsCountDS.foreachRDD { rdd =>
      //数据量较大时,都集中在driver端压力太大,所以可以分片存
      //每个分片中都有一堆的(day, areaTop3AdsCountJsonMap)
      rdd.foreachPartition { iterDayAreaTop3 =>

        val jedis: Jedis = MyRedisUtil.getJedisClient

        for ((day, areaAdsCountMap) <- iterDayAreaTop3) {

          //jedis不支持Scala的Map所以需要转换
          import collection.JavaConversions._
          jedis.hmset("top3_ads_per_day:" + day, areaAdsCountMap)
        }

        jedis.close()
      }
    }
  }
}
