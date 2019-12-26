package cn.whaley.sdk.dataopsio

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/5/12.
 * spark接口层
 */
trait SparkOpsIO {


  def getSparkConf:SparkConf

  def getSparkContext:SparkContext

  def setSparkConf(value:Map[String,String])

  def getSQLContext(scTmp: SparkContext):SQLContext

  def getSparkSession(appName: String):SparkSession
}
