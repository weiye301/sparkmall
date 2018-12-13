package com.weiye.sparkmall.realtime.app

import java.util
import java.util.Date

import com.weiye.sparkmall.common.util.{MyKafkaUtil, MyRedisUtil}
import com.weiye.sparkmall.realtime.bean.RealtimeAdsLog
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object RealtimeLogApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("realtime_ads").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)

    val adsStringDStream: DStream[String] = inputDStream.map(record => record.value())
    //    1.1 保存每个用户每天点击每个广告的次数
    //      每次rdd  -> 取出结果      redis[k,v]     key :     用户+日期+广告    value：点击次数
    //rdd[String] ->rdd[readtimelog]
    val realtimeLogDStream: DStream[RealtimeAdsLog] = adsStringDStream.map { adsString =>
      val fields: Array[String] = adsString.split(" ")

      val date: Date = new Date(fields(0).toLong)
      val area: String = fields(1)
      val city: String = fields(2)
      val userId: String = fields(3)
      val adsId: String = fields(4)
      RealtimeAdsLog(date, area, city, userId, adsId)
    }

    //    val filterRealtimeLogDStream: DStream[RealtimeAdsLog] = realtimeLogDStream.filter { realtimelog =>
    //      val jedisClient: Jedis = MyRedisUtil.getJedisClient
    //      val result: lang.Boolean = jedisClient.sismember("user_blacklist", realtimelog.userId)
    //      jedisClient.close()
    //      !result
    //    }
    //过滤掉黑名单中的用户日志
    val filterRealtimeLogDStream: DStream[RealtimeAdsLog] = realtimeLogDStream.transform { rdd =>
      val jedisClient: Jedis = MyRedisUtil.getJedisClient
      val blacklist: util.Set[String] = jedisClient.smembers("user_blacklist")
      jedisClient.close()
      val blacklistBC: Broadcast[util.Set[String]] = sc.broadcast(blacklist)
      rdd.filter { realtimlog =>
        !blacklistBC.value.contains(realtimlog.userId)
      }
    }

    //需求8
    //地区+城市+广告+天==>count
    val areaCityAdsDateDStream: DStream[(String, Long)] = filterRealtimeLogDStream.map { realtimelog =>
      (realtimelog.area + ":" + realtimelog.city + ":" + realtimelog.adsId + ":" + realtimelog.getDateString(), 1L)
    }.reduceByKey(_ + _)

    //把当前时间段的RDD里的值和之前的历史值相加
    sc.setCheckpointDir("./checkpoint")
    val adsCountSumDStream: DStream[(String, Long)] = areaCityAdsDateDStream.updateStateByKey { (adsCountSeq: Seq[Long], totalCount: Option[Long]) =>
      val adsCountSum: Long = adsCountSeq.sum
      val newTotalCount: Long = totalCount.getOrElse(0L) + adsCountSum
      Some(newTotalCount)
    }
    //把结果写出redis
    adsCountSumDStream.foreachRDD { rdd =>
      val totalCountArray: Array[(String, Long)] = rdd.collect()
      val jedisClient: Jedis = MyRedisUtil.getJedisClient
      for ((areaCityAdsDay, count) <- totalCountArray) {
        jedisClient.hset("area_city_ads_day_count", areaCityAdsDay, count.toString)
      }
      jedisClient.close()
    }

    //需求7
    //按天+用户+广告 进行聚合,计算点击量
    val userAdsCountPerDayDStream: DStream[(String, Long)]
    = filterRealtimeLogDStream.map { realtimelog =>
      val key: String = realtimelog.userId + ":" + realtimelog.adsId + ":" + realtimelog.getDateString()

      (key, 1L)
    }.reduceByKey(_ + _)

    //向redis中存放用户点击广告的累计值
    userAdsCountPerDayDStream.foreachRDD { rdd =>

      val userAdsCountArr: Array[(String, Long)] = rdd.collect()
      val jedisClient: Jedis = MyRedisUtil.getJedisClient

      for ((key, count) <- userAdsCountArr) {

        val countString: String = jedisClient.hget("user_ads_date_count", key)
        //达到阈值 进入黑名单
        if (countString != null && countString.toLong > 100) {
          //黑名单 结构 set
          jedisClient.sadd("user_blacklist", key.split(":")(0))
        }
        jedisClient.hincrBy("user_ads_date_count", key, count)
      }
      jedisClient.close()


    }

    //需求9
    AreaTop3AdsPerdayApp.calcTop3Ads(adsCountSumDStream)

    //需求10
    LastHourCountPerAds.calcLastHourCountPerAds(filterRealtimeLogDStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
