package cn.whaley.sdk.dataopsio

import redis.clients.jedis.JedisPoolConfig

/**
 * Created by Administrator on 2016/5/12.
 */
trait RedisOpsIO {


  def getJedisPoolConfig():JedisPoolConfig

  def getJedisPool()

  def getJedis()

  def getDataByKey(key: String)

  def getNumByKey(key: String)

  def setWaitTime(time:Long)

  def setMaxTotal(num:Int)




}
