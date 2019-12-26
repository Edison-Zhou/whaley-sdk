package cn.whaley.sdk.udf

import cn.whaley.sdk.dataOps.RedisOps

/**
 * Created by Administrator on 2016/6/6.
 */
object GetInfoFromRedis {

  def getVIPBySid(sid: String):String ={
    val redisDb = new RedisOps("redis_16_0")
    val data = redisDb.getVIPBySid(sid)
    redisDb.destroy()
    data
  }

  def getTitleBySid(sid: String):String={
    val redisDb = new RedisOps("redis_16_0")
    val data = redisDb.getTitleBySid(sid)
    redisDb.destroy()
    data
  }

  def getAreaBySid(sid: String):String={
    val redisDb = new RedisOps("redis_16_0")
    val data = redisDb.getAreaBySid(sid)
    redisDb.destroy()
    data
  }



}
