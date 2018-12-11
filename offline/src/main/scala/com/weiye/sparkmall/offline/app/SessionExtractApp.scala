package com.weiye.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.Date

import com.weiye.sparkmall.common.model.UserVisitAction
import com.weiye.sparkmall.offline.bean.SessionInfo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

import scala.util.Random

object SessionExtractApp {


  def sessionExreact(taskId: String, sessionCount: Long, sessionActionRDD: RDD[(String, Iterable[UserVisitAction])], extractNum: Long): RDD[SessionInfo] = {

    //  1  RDD[sessionId,Iterable[UserAction]]
    //  =>map=>
    val sessionInfoRDD: RDD[SessionInfo] = sessionActionRDD.map { case (sessionId, iterActions) =>
      //求时长和开始时间
      var maxTime: Long = -1L
      var minTime: Long = Long.MaxValue
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val keywordBuffer = new ListBuffer[String]()
      val clickBuffer = new ListBuffer[String]()
      val orderBuffer = new ListBuffer[String]()
      val payBuffer = new ListBuffer[String]()

      for (action <- iterActions) {
        val time: Long = format.parse(action.action_time).getTime
        maxTime = Math.max(time, maxTime)
        minTime = Math.min(time, minTime)

        //提取关键字,点击页面id,订单和支付
        if (action.search_keyword != null) {
          keywordBuffer += action.search_keyword
        } else if (action.click_product_id != -1L) {
          clickBuffer += action.click_product_id.toString
        } else if (action.order_product_ids != null) {
          orderBuffer += action.order_product_ids
        } else if (action.pay_product_ids != null) {
          payBuffer += action.pay_product_ids
        }

      }
      //时长
      val visitTime: Long = maxTime - minTime
      //开始时间
      val startTime: String = format.format(new Date(minTime))
      //步长
      val stepLenth: Int = iterActions.size

      SessionInfo(taskId, sessionId, startTime, stepLenth, visitTime, keywordBuffer.mkString(","), clickBuffer.mkString(","), orderBuffer.mkString(","), payBuffer.mkString(","))
    }

    //    2 RDD[ sessionInfo]
    //  =>map=>
    val dayHourSessionInfoRDD: RDD[(String, SessionInfo)] = sessionInfoRDD.map { sessionInfo =>
      val dayHour: String = sessionInfo.startTime.split(":")(0)
      (dayHour, sessionInfo)
    }
    //    3 RDD[ day_hour,sessionInfo]
    //  =>groupbykey
    val dayHourSessionInfoIterRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionInfoRDD.groupByKey()
    //  4 RDD[day_hour, Iterable[sessionInfo]]  多个
    //    =》抽取
    val resultRDD: RDD[SessionInfo] = dayHourSessionInfoIterRDD.flatMap { case (dayHour, iterSessions) =>
      //先算出要抽取的个数:
      val num: Long = Math.round(iterSessions.size.toDouble / sessionCount * extractNum)
      //抽取
      val result: mutable.HashSet[SessionInfo] = randomExtract(iterSessions.toArray, num)
      result
    }
    resultRDD


  }

  /**
    * 随机抽取指定数量
    *
    * @param arr arr
    * @param num 要抽取的个数
    * @tparam T 随意
    */
  def randomExtract[T](arr: Array[T], num: Long): mutable.HashSet[T] = {
    val resultSet = new mutable.HashSet[T]()
    while (resultSet.size < num) {
      val index: Int = new Random().nextInt(arr.length)
      resultSet += arr(index)
    }

    resultSet
  }
}
