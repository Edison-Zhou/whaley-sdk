package cn.whaley.sdk.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import cn.whaley.sdk.dataexchangeio.DataIO

/**
 *jobstatus create by xutong
 *
 */
object JobStatus {

  //定义时间变量
  var jobStartTime:String = ""
  var jobEndTime:String = ""

  //定义状态变量
  var schedule:String = ""
  var groupName: String = ""
  var taskName: String =""


  /**
   * 获取调度状态参数
   * @param appName
   */
  def getConfig(appName: String): Unit ={

    val tmp = appName.split("#")

    if(tmp.size == 3){
      schedule=tmp(0)
      groupName=tmp(1)
      taskName = tmp(2)
    }else{
      System.out.println("appName size is error")
      System.exit(-1)
    }

  }

  /**
   *任务开始，记录任务状态
   */
  def startRecordJobStatus(): Unit ={

    val dateTime = Calendar.getInstance().getTime
    val dateTimeFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    jobStartTime = dateTimeFormat.format(dateTime)

    val executeDate = jobStartTime.split(" ")(0)
    val executeTime = jobStartTime.split(" ")(1)
    val duration = 0
    val status = -1

    try{
      val scheduleMysql=DataIO.getMySqlOps("job_bi_mysql")

      val sqlInsert = "INSERT INTO schedulehistory(schedule,groupName,taskName,executeDate,"+
        "executeTime,duration,status,createdAt,updatedAt) VALUES(?,?,?,?,?,?,?,?,?)"

      scheduleMysql.insert(sqlInsert,schedule,groupName,taskName,executeDate,executeTime,
        duration,status,jobStartTime,jobStartTime)

      scheduleMysql.destory()
    }catch{
      case e:Exception => e.printStackTrace()
    }

  }

  /**
   * 记录结束时的任务状态
   */
  def endRecordJobStatus(status: Int): Unit ={

    val dateTimeFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val cal = Calendar.getInstance()
    cal.setTime(dateTimeFormat.parse(jobStartTime))
    val start = cal.getTimeInMillis

    val executeDate = jobStartTime.split(" ")(0)
    val executeTime = jobStartTime.split(" ")(1)

    val dateTime = Calendar.getInstance().getTime
    jobEndTime = dateTimeFormat.format(dateTime)
    cal.setTime(dateTimeFormat.parse(jobEndTime))
    val end = cal.getTimeInMillis

    val duration = (end - start)/1000

    try{
      val scheduleMysql=DataIO.getMySqlOps("job_bi_mysql")

      val sqlUpdate = s"UPDATE schedulehistory SET duration = $duration,status = $status,updatedAt = '$jobEndTime' WHERE"+
        s" schedule= '$schedule' AND groupName = '$groupName' AND taskName = '$taskName' AND "+
        s" executeDate = '$executeDate' AND executeTime = '$executeTime'"

      scheduleMysql.update(sqlUpdate)

      scheduleMysql.destory()
    }catch{
      case e:Exception => e.printStackTrace()
    }


  }

/*  //任务调度初始化
  def init()={}

  //执行调度任务
  def execute()={}

  //结束调度任务
  def destory()={}*/

}
