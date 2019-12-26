package cn.whaley.sdk.customize

import org.apache.spark.SparkConf
import org.json.JSONObject

/**
 * Created by xutong on 2016/3/15.
 * 读取spark的参数信息
 */
object Sparkconf{


  def SetSparkConf(sparkConfigData:String):SparkConf={
    val config = new SparkConf()

    val conf = new JSONObject(sparkConfigData).getJSONObject("sparkConf")
    val keySet = conf.keys()
    var key: String = ""
    var value: String =""
    while(keySet.hasNext){
      key = keySet.next.toString
      value = conf.getString(key)
      config.set(key,value)
    }
    config
  }
}
