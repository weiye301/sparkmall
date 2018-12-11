package com.weiye.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.weiye.sparkmall.common.model.UserVisitAction
import com.weiye.sparkmall.common.util.{ConfigurationUtil, JdbcUtil}
import com.weiye.sparkmall.offline.bean.{CategorySessionTop, CategoryTopN, SessionInfo}
import com.weiye.sparkmall.offline.util.{CategoryActionCountAccumulator, SessionAccumulator}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object SessionStatApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("offline")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val taskId: String = UUID.randomUUID().toString
    //sessionStepVisitTime(taskId, sparkSession)
    //1 \ 筛选  要关联用户  sql   join user_info  where  contidition  =>DF=>RDD[UserVisitAction]
    val userVisitActionRDD: RDD[UserVisitAction] = getUserVisitActionRDD(sparkSession)

    // 2  rdd=>  RDD[(sessionId,UserVisitAction)] => groupbykey => RDD[(sessionId,iterable[UserVisitAction])]
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map(userVisitAction => (userVisitAction.session_id, userVisitAction)).groupByKey()
    sessionRDD.cache()

    //3 求 session总数量，
    val sessionCount: Long = sessionRDD.count()

    //需求1
    val conditionJsonString: String = sessionStepVisitTime(taskId, sparkSession, sessionCount, userVisitActionRDD, sessionRDD)

    //需求2
    randomSession(taskId, sessionCount, sessionRDD, sparkSession)

    //需求3
    val categoryTop10: List[CategoryTopN] = CategoryTop10(taskId, sparkSession, userVisitActionRDD)

    //需求4
    statCategorySession(taskId, sparkSession, categoryTop10, userVisitActionRDD)

    //需求5
    pageConvertRatio(taskId, sparkSession, conditionJsonString, userVisitActionRDD)
  }

  /**
    * 需求5
    * 页面单跳转化率
    *
    * @param taskId              taskId
    * @param sparkSession        sparkSession
    * @param conditionJsonString JSON字符串,保存的是过滤要求
    * @param userVisitActionRDD  UserVisitAction的RDD
    */
  def pageConvertRatio(taskId: String, sparkSession: SparkSession, conditionJsonString: String, userVisitActionRDD: RDD[UserVisitAction]): Unit = {

    //    1 转化率的公式：  两个页面跳转的次数 /  前一个页面的访问次数
    //    2   1，2, 3,4,5,6,7  ->        1-2,2-3,3-4,4-5,5-6,6-7次数  /  1,2,3,4,5,6次数
    //取得要计算的跳转页面
    val pageVisitArray: Array[String] = JSON.parseObject(conditionJsonString).getString("targetPageFlow").split(",") //(1,2,3,4,5,6)
    val pageVisitArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageVisitArray.slice(0, pageVisitArray.length - 1))

    //      3  可以用zip 1-6 zip 2-7 ->  (1,2),(2,3),…….-> 1-2,2-3,3-4,4-5,5-6,6-7
    val pageJumpTupleArray: Array[(String, String)] = pageVisitArray.slice(0, pageVisitArray.length - 1).zip(pageVisitArray.slice(1, pageVisitArray.length)) //(1,2),(2,3)....

    val pageJumpArray: Array[String] = pageJumpTupleArray.map { case (page1, page2) => page1 + "-" + page2 } //1-2 2-3...
    val pageJumpArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageJumpArray)

    //    4 前一个页面的访问次数  1-6页面的访问次数  ->取pageid为1-6的访问次数
    //  pageid为1-6的访问记录 -> 按照pageid进行count-> countbykey  Map[key,count]
    //(1)先过滤,只要页面为1-6的
    val filtUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { action =>
      pageVisitArrayBC.value.contains(action.page_id.toString)
    }
    //(2)计算pageid1-6的次数
    val pageVisitCountMap: collection.Map[Long, Long] = filtUserVisitActionRDD.map { action => (action.page_id, 1L) }.countByKey()

    //    5  两个页面跳转的次数
    //      根据sessionId进行聚合   RDD[sessionId,iterable[UserVisitAction]]
    val userActionBySessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map { action =>
      (action.session_id, action)
    }.groupByKey()

    //    =>针对每个session的访问记录按时间排序 sortwith=>List[pageId]// 1,2,4,5,6,7,8,12,13,16
    //    1-13 zip 2-16 -> 1-2,2-4,4-5,5-6……->(1-2,1L)-> 进行一次过滤 -> reducebykey  (1-2,300L),(2-3,120L)….
    //    =》   1-2,2-3,3-4,4-5,5-6,6-7次数  Map[key,count]
    val pageJumpRDD: RDD[String] = userActionBySessionRDD.flatMap { case (sessionId, actions) =>
      //将每个sessionId里的内容按时间排序  正序
      val actionSortedList: List[UserVisitAction] = actions.toList.sortWith((action1, action2) => action1.action_time < action2.action_time)
      //将上面的list过滤为只有pageId的list
      val pageIdList: List[Long] = actionSortedList.map { action => action.page_id }
      //将pageIdList变为1-2 2-3的格式
      val pageJumpList: List[String] = pageIdList.slice(0, pageIdList.length - 1).zip(pageIdList.slice(1, pageIdList.length)).map { case (page1, page2) => page1 + "-" + page2 }
      pageJumpList
    }


    //过滤为只剩下符合要求(pageId为1-6)的list
    val filteredPageJumpRDD: RDD[String] = pageJumpRDD.filter { pageJumps =>
      pageJumpArrayBC.value.contains(pageJumps)
    }

    //计算得出每个跳转的次数
    val pageJumpCountMap: collection.Map[String, Long] = filteredPageJumpRDD.map { pageJump => (pageJump, 1L) }.countByKey()

    //    6 两个map 分别进行除法得到 转化率
    val pageConvertRatios: Iterable[Array[Any]] = pageJumpCountMap.map { case (pageJump, count) =>
      val prefixPageId: String = pageJump.split("-")(0)
      val prefixPageCount: Long = pageVisitCountMap.getOrElse(prefixPageId.toLong, 0L)
      val ratio: Double = Math.round(count / prefixPageCount.toDouble * 1000) / 10.0
      Array(taskId, pageJump, ratio)
    }

    //      7 存库
    JdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?)", pageConvertRatios)
  }

  /**
    * 需求4:
    *
    * @param taskId             taskId
    * @param sparkSession       sparkSession
    * @param categoryTop10      categoryTop10 List
    * @param userVisitActionRDD userVisitActionRDD
    */
  def statCategorySession(taskId: String, sparkSession: SparkSession, categoryTop10: List[CategoryTopN], userVisitActionRDD: RDD[UserVisitAction]): Unit = {

    //    1、	过滤: 过滤出所有排名前十品类的action=>RDD[UserVisitAction]
    val filterUserActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userAction =>
      var flag = false
      for (categoryTop <- categoryTop10) {
        if (userAction.click_category_id.toString == categoryTop.categoryId) flag = true
      }
      flag
    }

    //      2 相同的cid+sessionId进行累加计数
    //      RDD[userAction.clickcid+userAction.sessionId,1L]
    //    reducebykey(_+_)  =>RDD[cid_sessionId,count]
    val cidSessionIdCountRDD: RDD[(String, Long)] = filterUserActionRDD.map { userAction =>
      (userAction.click_category_id + "_" + userAction.session_id, 1L)
    }.reduceByKey(_ + _)

    //    3、根据cid进行聚合
    //    RDD[cid_sessionId,count]
    //    =>RDD[cid,(sessionId,count)]
    //    => groupbykey => RDD[cid,Iterable[(sessionId,clickCount)]]
    val sessionIdCountByCidRDD: RDD[(String, Iterable[(String, Long)])] = cidSessionIdCountRDD.map { case (cid_sessionId, count) =>
      val cidSessionIdArr: Array[String] = cid_sessionId.split("_")
      val cid: String = cidSessionIdArr(0)
      val sessionId: String = cidSessionIdArr(1)
      (cid, (sessionId, count))
    }.groupByKey()

    //    4 、聚合后进行排序、截取
    //    =>RDD[cid,Iterable[(sessionId,clickCount)]]
    //    =>把iterable 进行排序截取前十=>RDD[cid,Iterable[(sessionId,clickCount)]]
    //    5、 结果转换成对象
    val categorySessionTopRDD: RDD[CategorySessionTop] = sessionIdCountByCidRDD.flatMap { case (cid, iterSessionCount) =>
      val sessionCountTop10: List[(String, Long)] = iterSessionCount.toList.sortWith { (sessionCount1, sessionCount2) =>
        sessionCount1._2 > sessionCount2._2
      }.take(10)
      val categorySessionTopList: List[CategorySessionTop] = sessionCountTop10.map { case (session, count) =>
        CategorySessionTop(taskId, cid, session, count)
      }
      categorySessionTopList
    }

    //    6、存储到数据库中
    //    =>RDD[CategorySessionTop]=>存数据库
    import sparkSession.implicits._
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    categorySessionTopRDD.toDF.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", "category_top10_session_count")
      .mode(SaveMode.Append).save()
  }

  /**
    * 需求三:根据点击,下单,支付进行排序,获取投票Top10
    *
    * @param taskId             taskId
    * @param sparkSession       sparkSession
    * @param userVisitActionRDD userVisitActionRDD
    * @return categoryTop10 需求四需要
    */
  def CategoryTop10(taskId: String, sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction]): List[CategoryTopN] = {
    //需求三  根据点击、下单、支付进行排序，取前十名

    //    累加器 hashMap[String ,Long]
    val categoryActionCountAccumulator = new CategoryActionCountAccumulator()
    sparkSession.sparkContext.register(categoryActionCountAccumulator)

    //    1 、遍历所有的访问日志
    //    map
    //    2 按照cid+操作类型进行分别累加
    userVisitActionRDD.foreach { userAction =>
      if (userAction.click_category_id != -1L) {
        //累加点击该品类的次数
        categoryActionCountAccumulator.add(userAction.click_category_id + "_click")
      } else if (userAction.order_category_ids != null) {
        //累加下单品类次数
        //由于下单涉及多个品类,所以想分割,然后循环进行累加
        val orderCidArray: Array[String] = userAction.order_category_ids.split(",")
        for (orderCid <- orderCidArray) {
          categoryActionCountAccumulator.add(orderCid + "_order")
        }
      } else if (userAction.pay_category_ids != null) {
        //累加支付的品类次数
        //有意支付也涉及多个品类,先分割,在循环累加
        val payCidArr: Array[String] = userAction.pay_category_ids.split(",")
        for (payCid <- payCidArr) {
          categoryActionCountAccumulator.add(payCid + "_pay")
        }
      }
    }

    //      3  得到累加的结果  map[cid_actiontype,count]
    //    累加器 hashMap[String ,Long]
    val countByCidMap: Map[String, mutable.HashMap[String, Long]] = categoryActionCountAccumulator.value.groupBy { case (cidType, count) => //定义用什么来分组
      val cid: String = cidType.split("_")(0)
      cid
    }

    //4 把结果转 cid,clickcount,ordercount,paycount
    val categoryTopNList: List[CategoryTopN] = countByCidMap.map { case (cid, actionMap) =>
      CategoryTopN(taskId, cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
    }.toList

    //5.将List中的count按要求排序,取前十个
    val categoryTop10: List[CategoryTopN] = categoryTopNList.sortWith((ctn1, ctn2) =>
      if (ctn1.clickCount > ctn2.clickCount) {
        true
      } else if (ctn1.clickCount == ctn2.clickCount) {
        if (ctn1.orderCount > ctn2.orderCount) {
          true
        } else {
          false
        }
      } else {
        false
      }
    ).take(10)

    //6.将数据插入数据库
    val categoryList = new ListBuffer[Array[Any]]
    for (top10 <- categoryTop10) {
      val paramArray = Array(top10.taskId, top10.categoryId, top10.clickCount, top10.orderCount, top10.payCount)
      categoryList.append(paramArray)
    }
    JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?) ", categoryList)

    categoryTop10
  }

  /**
    * 需求2
    *
    * @param taskId       taskId
    * @param sessionCount 总session个数
    * @param sessionRDD   sessionRDD
    * @param sparkSession sparkSession
    */
  def randomSession(taskId: String, sessionCount: Long, sessionRDD: RDD[(String, Iterable[UserVisitAction])], sparkSession: SparkSession): Unit = {
    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExreact(taskId, sessionCount, sessionRDD, 1000)
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    import sparkSession.implicits._
    sessionExtractRDD.toDF.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", "random_session_info")
      .mode(SaveMode.Append).save()
  }

  /**
    * 需求1
    *
    * @param taskId       taskId
    * @param sparkSession sparkSession
    */
  def sessionStepVisitTime(taskId: String, sparkSession: SparkSession, sessionCount: Long, userVisitActionRDD: RDD[UserVisitAction], sessionRDD: RDD[(String, Iterable[UserVisitAction])]): String = {

    val accumulator = new SessionAccumulator()
    sparkSession.sparkContext.register(accumulator)

    //    遍历一下全部session，对每个session的类型进行判断 来进行分类的累加  （累加器）
    //    4  分类 ：时长 ，把session里面的每个action进行遍历 ，取出最大时间和最小事件 ，求差得到时长 ，再判断时长是否大于10秒
    //    步长： 计算下session中有多少个action, 判断个数是否大于5
    sessionRDD.foreach { case (sessionId, actions) =>
      var maxTime: Long = -1L
      var minTime: Long = Long.MaxValue
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      for (action <- actions) {
        val actionTime: Long = format.parse(action.action_time).getTime
        maxTime = Math.max(actionTime, maxTime)
        minTime = Math.min(actionTime, minTime)
      }
      //时长
      val visitTime: Long = maxTime - minTime
      if (visitTime > 10000) {
        accumulator.add("session_visitLength_gt_10_count")
      } else {
        accumulator.add("session_visitLength_le_10_count")
      }
      //步长
      if (actions.size > 5) {
        accumulator.add("session_stepLength_gt_5_count")
      } else {
        accumulator.add("session_stepLength_le_5_count")
      }

    }
    //5 提取累加器中的值
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value
    //println(sessionCountMap.mkString("---"))
    //6 把累计值计算为比例
    val session_visitLength_gt_10_ratio: Double = Math.round(10000.0 * sessionCountMap("session_visitLength_gt_10_count") / sessionCount) / 100.0
    val session_visitLength_le_10_ratio: Double = Math.round(10000.0 * sessionCountMap("session_visitLength_le_10_count") / sessionCount) / 100.0
    val session_stepLength_gt_5_ratio: Double = Math.round(10000.0 * sessionCountMap("session_stepLength_gt_5_count") / sessionCount) / 100.0
    val session_stepLength_le_5_ratio: Double = Math.round(10000.0 * sessionCountMap("session_stepLength_le_5_count") / sessionCount) / 100.0

    //    println(session_visitLength_gt_10_ratio)
    //    println(session_visitLength_le_10_ratio)
    //    println(session_stepLength_gt_5_ratio)
    //    println(session_stepLength_le_5_ratio)
    //7 保存到mysql中
    val conditionConfig: FileBasedConfiguration = ConfigurationUtil("condition.properties").config
    val conditionJson: String = conditionConfig.getString("condition.params.json")

    val resultArray = Array(taskId, conditionJson, sessionCount, session_visitLength_le_10_ratio, session_visitLength_gt_10_ratio, session_stepLength_le_5_ratio, session_stepLength_gt_5_ratio)
    JdbcUtil.executeUpdate("insert into session_stat_info values (?,?,?,?,?,?,?) ", resultArray)
    conditionJson
  }

  /**
    * 根据配置文件condition.properties中的过滤条件将数据过滤,再将过滤后的数据转为RDD[UserVisitAction]
    *
    * @param sparkSession sparkSession
    * @return RDD[UserVisitAction] rdd
    */
  def getUserVisitActionRDD(sparkSession: SparkSession): RDD[UserVisitAction] = {
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")
    sparkSession.sql("use " + databaseName)

    val conditionConfig: FileBasedConfiguration = ConfigurationUtil("condition.properties").config
    val conditionJson: String = conditionConfig.getString("condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(conditionJson)

    val sql = new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1")

    if (jSONObject.getString("startDate") != null) {
      sql.append(" and date>='" + jSONObject.getString("startDate") + "'")
    }

    if (jSONObject.getString("endDate") != null) {
      sql.append(" and date<='" + jSONObject.getString("endDate") + "'")
    }

    if (jSONObject.getString("startAge") != null) {
      sql.append(" and u.age>=" + jSONObject.getString("startAge"))
    }
    if (jSONObject.getString("endAge") != null) {
      sql.append(" and u.age<=" + jSONObject.getString("endAge"))
    }
    if (!jSONObject.getString("professionals").isEmpty) {
      sql.append(" and u.professional in (" + jSONObject.getString("professionals") + ")")
    }
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
    rdd
  }
}
