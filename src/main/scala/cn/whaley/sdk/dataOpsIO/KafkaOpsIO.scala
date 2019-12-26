package cn.whaley.sdk.dataopsio

import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2016/5/12.
 */
trait KafkaOpsIO {

  def setKafkaConf(conf:Map[String,String])

 // def getKafkaConf:ProducerConfig

  def writeRDD2Kafka(data: RDD[String],topic: String)

  def writeLocal2Kafka(data: Seq[String],topic: String)

  def writrString2Kafka(data: String,topic: String)

  def writeData2KafkaByKey(data: String,topic: String,key:String)


}
