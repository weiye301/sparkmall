package com.weiye.sparkmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class RealtimeAdsLog(logdate: Date, area: String, city: String, userId: String, adsId: String) {

  def getDateTimeString() = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(logdate)
  }

  def getDateString() = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.format(logdate)
  }

  def getHourMinuString() = {
    val format = new SimpleDateFormat("HH:mm")
    format.format(logdate)
  }

}
