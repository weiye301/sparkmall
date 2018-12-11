package com.weiye.sparkmall.mock.util

import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //  用delimiter分割  canRepeat为false则不允许重复
    if (canRepeat) {
      val valueList = new ListBuffer[Int]()

      for (i <- 1 to amount) {
        val randomNum = apply(fromNum, toNum)
        valueList += randomNum
      }
      valueList.mkString(",")
    } else {
      val valueSet = new mutable.HashSet[Int]()
      while (valueSet.size < amount) {
        val randomSet = apply(fromNum, toNum)
        valueSet += randomSet
      }
      valueSet.mkString(",")

    }
  }

  def main(args: Array[String]): Unit = {
    println(multi(1, 5, 3, ",", true))
  }


}

