package cn.whaley.sdk.dataopsio

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * Created by Administrator on 2016/5/12.
 */
trait HDFSOpsIO {


  def getDataByTodayNum(dataType:String, logType:String,numOfDays:Int
                         )(implicit sqlContext:SQLContext):DataFrame

  def getDataByTodayNumSS(dataType:String, logType:String,numOfDays:Int
                         )(implicit ss: SparkSession):DataFrame

  def getNumOfData(startDate:String, endDate:String):Int

  def getDataByNumForRecommend(dataType:String, logType:String,startDate:String, numOfDays:Int
                                )(implicit sqlContext:SQLContext):DataFrame

  def getDataByNumForRecommendSS(dataType:String, logType:String,startDate:String, numOfDays:Int
                                )(implicit ss: SparkSession):DataFrame

  def getDataByDateNum(dataType:String, logType:String,startDate:String, numOfDays:Int
                        )(implicit sqlContext:SQLContext):DataFrame

  def getDataByDateNumSS(dataType:String, logType:String,startDate:String, numOfDays:Int
                        )(implicit ss: SparkSession):DataFrame

  def getDataByStopDay(dataType:String, logType:String,startDate:String, endDate:String
                        )(implicit sqlContext:SQLContext):DataFrame

  def getDataByStopDaySS(dataType:String, logType:String,startDate:String, endDate:String
                        )(implicit ss: SparkSession):DataFrame

  def getDFByDateNumWithSql(sql: String,dataType:String, logType:String,startDate:String, numOfDays:Int
                             )(implicit sqlContext: SQLContext):DataFrame

  def getDFByDateNumWithSqlSS(sql: String,dataType:String, logType:String,startDate:String, numOfDays:Int
                             )(implicit ss: SparkSession):DataFrame

  def getDFByStopDateWithSql(sql: String,dataType:String, logType:String,startDate:String, endDate:String
                              )(implicit sqlContext: SQLContext):DataFrame

  def getDFByStopDateWithSqlSS(sql: String,dataType:String, logType:String,startDate:String, endDate:String
                              )(implicit ss: SparkSession):DataFrame

  def deleteHDFSFile(file: String)

  def existsFile(file: String):Boolean

  def saveFileToHDFS(data: RDD[_], path: String, format: String)

}
