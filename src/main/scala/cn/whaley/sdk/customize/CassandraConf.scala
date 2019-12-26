package cn.whaley.sdk.customize

import java.util.Properties

import org.json.JSONObject

/**
 * Created by Administrator on 2016/5/19.
 */
object CassandraConf {


  def SetCassandraConf(CassandraConfData:String):Properties={
    val props = new Properties()
    val conf = new JSONObject(CassandraConfData).getJSONObject("cassandraConf")
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
