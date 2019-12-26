package cn.whaley.sdk.customize

import java.util.Properties

import cn.whaley.sdk.parse.ReadConfig
import org.apache.spark.SparkConf
import org.json.JSONObject

/**
 * Created by xutong on 2016/3/15.
 * 获取各参数属性
 */
object TotalConf {

  lazy val configData: String = ReadConfig.allConfigData

  /**
   * 获取spark的参数信息
   * @return sparkconf
   */
  def setSparkConf:SparkConf={
    val conf = Sparkconf.SetSparkConf(configData)
    conf
  }

  /**
   * 获取cassandra的参数信息
   * @return
   */
  def setCassandraConf:Properties={
    val conf = CassandraConf.SetCassandraConf(configData)
    conf
  }

  /**
   * 获取mysql的参数信息
   * @param DB_type  mysql的映射名
   * @return  返回mysql的参数信息jsonobject格式
   */
  def setDBConf(DB_type: String):Properties={
    val props = new Properties()
    val conf = new JSONObject(configData).getJSONObject(DB_type)
    val keySet = conf.keys()
    var key: String = ""
    var value: String =""
    while(keySet.hasNext){
      key = keySet.next.toString
      value = conf.getString(key)
      props.put(key,value)
    }
    props
  }

  /**
   * 获取redis的参数信息
   * @param redis_type redis的映射名
   * @return 返回redis的参数信息 jsonobject格式
   */
  def setRedisConf(redis_type: String):Properties={
    val props = new Properties()
    val conf = new JSONObject(configData).getJSONObject(redis_type)
    val keySet = conf.keys()
    var key: String = ""
    var value: String =""
    while(keySet.hasNext){
      key = keySet.next.toString
      value = conf.getString(key)
      props.put(key,value)
    }
    props
  }

  /**
   * 获取kafka的参数信息
   * @return kafkaconf
   */
  def setKafkaConf:Properties={
    val conf = KafkaConf.SetKafkaConf(configData)
    conf
  }

}

