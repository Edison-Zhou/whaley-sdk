package cn.whaley.sdk.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Created by Administrator on 2016/8/23.
 */
object GetFileListFromHDFS {

  private val dateFormat = new SimpleDateFormat("yyyyMMdd")
  lazy val cal = Calendar.getInstance()
  lazy val today = dateFormat.format(cal.getTime)

  val dataTypeSet = Set("moretv","helios","medusa","merge")
  /**
   * 获取起止时间间的天数
   * @param startDate  开始时间
   * @param endDate   截止时间
   * @return
   */
  def getNumOfData(startDate:String, endDate:String):Int = {

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(dateFormat.parse(startDate))
    val start = cal.getTimeInMillis

    cal.setTime(dateFormat.parse(endDate))
    val end = cal.getTimeInMillis

    require(start > end,"end date need to be after start date")

    val dayNum = (start - end)/(1000*3600*24)

    val day = Integer.parseInt(String.valueOf(dayNum))

    day
  }

  /**
   *  提取当天开始的hdfs上文件列表信息
   * @param dataType 数据类型
   * @param logType 日志类型
   * @param numOfDays 天数
   * @return
   */
  def getFileListByTodayNum(dataType:String, logType:String,numOfDays:Int
                             ): Array[String] = {
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(today))
    val inputs = new Array[String](numOfDays)

    require(dataTypeSet.contains(dataType),"dataType need to be moretv or helios or medusa")
    dataType match{
      case "moretv" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/mbi/parquet/$logType/$date/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "helios" =>
        calendar.add(Calendar.DAY_OF_MONTH,-1)
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/whaley/parquet/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "medusa" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/medusa/parquet/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "merge" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/medusaAndMoretvMerger/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
    }
    inputs
  }

  def getFileListByNumForRecommend(dataType:String, logType:String,startDate:String, numOfDays:Int): Array[String] ={
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(startDate))
    val inputs = new Array[String](numOfDays)

    require(dataTypeSet.contains(dataType),"dataType need to be moretv or helios or medusa")
    dataType match{
      case "moretv" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/mbi/parquet/$logType/$date/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "helios" =>
        calendar.add(Calendar.DAY_OF_MONTH,-1)
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/whaley/parquet/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "medusa" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/medusa/parquet/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "merge" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/medusaAndMoretvMerger/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
    }
    inputs
  }

  def getFileListByDataNum(dataType:String, logType:String,startDate:String, numOfDays:Int):Array[String]={
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(startDate))
    val inputs = new Array[String](numOfDays)

    require(dataTypeSet.contains(dataType),"dataType need to be moretv or helios or medusa")
    dataType match{
      case "moretv" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/mbi/parquet/$logType/$date/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "helios" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/whaley/parquet/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "medusa" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/medusa/parquet/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
      case "merge" =>
        for(i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/log/medusaAndMoretvMerger/$date/$logType/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
    }
    inputs
  }
}
