package com.weiye.sparkmall.realtime.app

import com.weiye.sparkmall.common.util.MyRedisUtil
import com.weiye.sparkmall.realtime.bean.RealtimeAdsLog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

object LastHourCountPerAds {

  def calcLastHourCountPerAds(filterRealtimeLogDStream: DStream[RealtimeAdsLog]): Unit = {

    //    RDD[Realtimelog]=> window=>map=>  利用滑动窗口去最近一小时的日志
    val lastHourCountPerAdsDS: DStream[RealtimeAdsLog] = filterRealtimeLogDStream.window(Minutes(60), Seconds(10))

    //      RDD[(adsid_hour_min,1L)] -> reducebykey  //按照每个广告+小时分钟进行统计个数
    //    =>RDD[adsid_hour_min,count]     按广告id进行聚合，把本小时内相同广告id的计数聚到一起
    val hourMinuCountByAdsDS: DStream[(String, Long)] = lastHourCountPerAdsDS.map { realtimelog =>
      val adsId: String = realtimelog.adsId
      val hourMinute: String = realtimelog.getHourMinuString()
      val key: String = adsId + "_" + hourMinute
      (key, 1L)
    }.reduceByKey(_ + _)

    //    =>RDD[(adsId,(hour_min,count)] .groupbykey
    val hourMinuteCountByAdsGroupDS: DStream[(String, Iterable[(String, Long)])] = hourMinuCountByAdsDS.map { case (adsHourMinute, count) =>
      val fields: Array[String] = adsHourMinute.split("_")
      val adsId: String = fields(0)
      val hourMinute: String = fields(1)
      (adsId, (hourMinute, count))
    }.groupByKey()

    //    => RDD[adsId,Iterable[(hour_min,count)]]
    //RDD[adsId,iterable[(hourMinu,count)]]  //把小时分钟的计数变成json
    val adsJsonDS: DStream[(String, String)] = hourMinuteCountByAdsGroupDS.map { case (ads, iterHourMinuteCount) =>
      val houtMinuteCountList: List[(String, Long)] = iterHourMinuteCount.toList
      val jsonString: String = compact(render((houtMinuteCountList)))
      (ads, jsonString)
    }

    // 保存到redis中
    val jedis: Jedis = MyRedisUtil.getJedisClient
    adsJsonDS.foreachRDD { rdd => {
      val adsJsonstrArr: Array[(String, String)] = rdd.collect()
      import collection.JavaConversions._
      jedis.hmset("last_hour_ads_click", adsJsonstrArr.toMap)
    }
    }
    jedis.close()
  }
}
