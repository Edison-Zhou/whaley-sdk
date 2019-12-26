package cn.whaley.sdk.parse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.io.Source


/**
 * Created by xutong on 2016/3/15.
 * 解析配置文件
 * @return 返回一个JSONObject
 */
object ReadConfig {

  /**
   * 从HDFS上读取配置文件
   * @return jsonobjcet格式的配置信息
   */
  private def getConfg = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val in = fs.open(new Path("hdfs://hans/libs/config/aisdk-config2.11_1.0.6.json"))
    val confStream = Source.fromInputStream(in)
    val configData = confStream.getLines().mkString("")
    configData
  }
  val allConfigData = getConfg
}
