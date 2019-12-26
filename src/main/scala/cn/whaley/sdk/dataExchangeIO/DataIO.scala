package cn.whaley.sdk.dataexchangeio

import cn.whaley.sdk.dataOps._

/**
 * Created by Administrator on 2016/5/12.
 */
object DataIO {

  def getMySqlOps(mySqlName: String)={

    val mySqlOps = new MySqlOps(mySqlName)
    mySqlOps
  }

  def getRedisOps(redisName: String)={
    val redisOps = new RedisOps(redisName)
    redisOps
  }

  def getKafkaOps={
    KafkaOps
  }

  def getHDFSOps={
    HDFSOps
  }

  def getSparkOps={
    SparkOps
  }

  def getCassandraOps={
    CassandraOps
  }
}
