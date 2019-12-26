package cn.whaley.sdk.dataOps

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataopsio.HDFSOpsIO
import cn.whaley.sdk.utils.GetFileListFromHDFS
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * Created by xutong on 2016/3/15.
 * hdfs的工具类
 */
object HDFSOps extends HDFSOpsIO{

  private val dateFormat = new SimpleDateFormat("yyyyMMdd")
  lazy val cal = Calendar.getInstance()
  lazy val today = dateFormat.format(cal.getTime)

  val dataTypeSet = Set("moretv","helios","medusa","merge")

  /**
   * 提取当天开始的数据
   * @param dataType  数据类型 moretv or helios
   * @param logType     日志类型
   * @param numOfDays    天数
   * @return
   */
   override def getDataByTodayNum(dataType:String, logType:String,numOfDays:Int=1
                         )(implicit sqlContext:SQLContext):DataFrame ={
    val dayList = GetFileListFromHDFS.getFileListByTodayNum(dataType,logType,numOfDays).
      filter(path=>existsFile(path.split("\\*")(0)))
    sqlContext.read.parquet(dayList:_*)
  }

   override def getDataByTodayNumSS(dataType:String, logType:String,numOfDays:Int=1
                                    )(implicit ss:SparkSession):DataFrame={
     val dayList = GetFileListFromHDFS.getFileListByTodayNum(dataType,logType,numOfDays).
       filter(path=>existsFile(path.split("\\*")(0)))
    ss.read.parquet(dayList:_*)
  }

  /**
   * 获取起止时间间的天数
   * @param startDate  开始时间
   * @param endDate   截止时间
   * @return
   */
  override def getNumOfData(startDate:String, endDate:String):Int = {

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
   * 根据时间天数从HDFS上获取推荐数据
   * @param dataType 数据类型 moretv or heilos
   * @param logType   日志类型
   * @param startDate  起始时间
   * @param numOfDays   读入数据的天数，默认1天
   * @return  返回读取的数据
   */
  override def getDataByNumForRecommend(dataType:String, logType:String,startDate:String, numOfDays:Int=1
                                )(implicit sqlContext:SQLContext):DataFrame ={
    val dayList = GetFileListFromHDFS.getFileListByNumForRecommend(dataType,logType,startDate,numOfDays).
      filter(path=>existsFile(path.split("\\*")(0)))
    sqlContext.read.parquet(dayList:_*)
  }

  override def getDataByNumForRecommendSS(dataType:String, logType:String,startDate:String, numOfDays:Int=1
                                           )(implicit ss:SparkSession):DataFrame={
    val dayList = GetFileListFromHDFS.getFileListByNumForRecommend(dataType,logType,startDate,numOfDays).
      filter(path=>existsFile(path.split("\\*")(0)))
    ss.read.parquet(dayList:_*)
  }

  /**
   * 根据时间天数从HDFS上获取dataFrame
   * @param dataType 数据类型 moretv or heilos
   * @param logType   日志类型
   * @param startDate  起始时间
   * @param numOfDays   读入数据的天数，默认1天
   * @return  返回读取的数据
   */
  override def getDataByDateNum(dataType:String, logType:String,startDate:String, numOfDays:Int=1
                     )(implicit sqlContext:SQLContext):DataFrame ={
    val dayList = GetFileListFromHDFS.getFileListByDataNum(dataType,logType,startDate,numOfDays).
      filter(path=>existsFile(path.split("\\*")(0)))
    sqlContext.read.parquet(dayList:_*)
  }

  override def getDataByDateNumSS(dataType:String, logType:String,startDate:String, numOfDays:Int=1
                                 )(implicit ss:SparkSession):DataFrame ={
    val dayList = GetFileListFromHDFS.getFileListByDataNum(dataType,logType,startDate,numOfDays).
      filter(path=>existsFile(path.split("\\*")(0)))
    ss.read.parquet(dayList:_*)
  }

  /**
   * 根据起止时间返回数据
   * @param dataType 数据类型 moretv or heilos
   * @param logType   日志类型
   * @param startDate  起始时间
   * @param endDate    停止时间
   * @return
   */
  override def getDataByStopDay(dataType:String, logType:String,startDate:String, endDate:String
                        )(implicit sqlContext:SQLContext):DataFrame ={
    getDataByDateNum(dataType,logType,startDate,getNumOfData(startDate,endDate))
  }

  override def getDataByStopDaySS(dataType:String, logType:String,startDate:String, endDate:String
                                 )(implicit ss:SparkSession):DataFrame ={
    getDataByDateNumSS(dataType,logType,startDate,getNumOfData(startDate,endDate))
  }

  /**
   * 通过Sql语句和天数从HDFS上获取数据
   * @param sql  sql查询语句
   * @param dataType   数据类型 moretv or heilos
   * @param logType     日志类型
   * @param startDate   起始时间
   * @param numOfDays     时间天数 默认1天
   * @return   返回数据
   */

  override def getDFByDateNumWithSql(sql: String,dataType:String, logType:String,startDate:String, numOfDays:Int=1
                          )(implicit sqlContext: SQLContext):DataFrame={
    val df = getDataByDateNum(dataType,logType,startDate,numOfDays)
    df.registerTempTable("log_data")
    sqlContext.sql(sql)
  }

  override def getDFByDateNumWithSqlSS(sql: String,dataType:String, logType:String,startDate:String, numOfDays:Int=1
                                      )(implicit ss:SparkSession):DataFrame={
    val df = getDataByDateNumSS(dataType,logType,startDate,numOfDays)
    df.registerTempTable("log_data")
    ss.sql(sql)
  }


  /**
   * 通过Sql语句和起止时间从HDFS上获取数据
   * @param sql  sql查询语句
   * @param dataType   数据类型 moretv or heilos
   * @param logType     日志类型
   * @param startDate   起始时间
   * @param endDate     停止时间
   * @return   返回数据
   */
  override def getDFByStopDateWithSql(sql: String,dataType:String, logType:String,startDate:String, endDate:String
                              )(implicit sqlContext: SQLContext):DataFrame ={
    val df = getDataByStopDay(dataType,logType,startDate,endDate)
    df.registerTempTable("log_data")
    sqlContext.sql(sql)
  }

  override def getDFByStopDateWithSqlSS(sql: String,dataType:String, logType:String,startDate:String, endDate:String
                                       )(implicit ss: SparkSession):DataFrame ={
    val df = getDataByStopDaySS(dataType,logType,startDate,endDate)
    df.registerTempTable("log_data")
    ss.sql(sql)
  }

  /**
   * 删除hadfs上的文件
   * @param file 文件名
   */
  override def deleteHDFSFile(file: String) {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val path = new Path(file)
    if (fs.exists(path)) {
      val isDel = fs.delete(path, true)
    }
  }

  /**
   * 判定hdfs上文件是否存在
   * @param file  文件名
   * @return  存在返回true
   */
  override def existsFile(file: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(file)
    fs.exists(path)
  }

  /**
   * 将RDD存储到HDFS上
   * @param data 数据
   * @param path  存储路径
   * @param format  存储文件的格式
   */
  override def saveFileToHDFS(data: RDD[_], path: String, format: String = "text"): Unit = {

    require(Set("text","object").contains(format),"format need to be text or object")

    if (!existsFile(path)) {
      format match {
        case "text" =>
          data.saveAsTextFile(path)
        case "object" =>
          data.saveAsObjectFile(path)
      }
    }else{}
  }
}
