package cn.whaley.sdk.customize

import java.util.Properties

import org.json.JSONObject

/**
 * Created by xutong on 2016/3/16.
 * 获取kafka的参数信息
 */
object KafkaConf {

  def SetKafkaConf(kafkaConfData:String):Properties={
    val props = new Properties()
    val conf = new JSONObject(kafkaConfData).getJSONObject("kafkaConf")
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

}
