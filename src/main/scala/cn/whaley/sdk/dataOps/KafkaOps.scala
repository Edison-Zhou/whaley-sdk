package cn.whaley.sdk.dataOps

import java.util.Properties

import cn.whaley.sdk.dataopsio.KafkaOpsIO
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD

/**
 * Created by xutong on 2016/3/16.
 * kafka的工具类
 */
object KafkaOps extends KafkaOpsIO{

  //private val props = TotalConf.setKafkaConf
  private val props = new Properties()

  props.put("bootstrap.servers", "bigdata-appsvr-130-1:9093,bigdata-appsvr-130-2:9093,bigdata-appsvr-130-3:9093," +
    "bigdata-appsvr-130-4:9093,bigdata-appsvr-130-5:9093,bigdata-appsvr-130-6:9093")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  props.put("request.required.acks", "1")
  props.put("producer.type", "async")
  props.put("batch.num.messages", "100")
  props.put("compression.codec", "snappy")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  /**
   * 设置kafka参数
   * @param conf  新的kafka参数
   */
  override def setKafkaConf(conf:Map[String,String]): Unit ={
    conf.keys.foreach(x => {
      props.setProperty(x,conf(x))
    })
  }

  /**
   * 获取kafka参数
   * @return kafkaProps
   */
/*  override def getKafkaConf()={
    val kafkaConf = new ProducerConfig(props)
    kafkaConf
  }*/

  /**
   * 将RDD数据写入kafka
   * @param data   输入数据
   * @param topic  主题名
   */
  override def writeRDD2Kafka(data: RDD[String],topic: String): Unit ={
    data.foreachPartition(dataTmp => {

      val producerEach = new KafkaProducer[String,String](props)
      dataTmp.foreach(x=>{
      val msg = new ProducerRecord[String,String](topic,Math.random()+"",x)
        producerEach.send(msg)
        })
      producerEach.close()
    })
  }

  /**
   * 将本地化的数据写入kafka
   * @param data   输入数据
   * @param topic   主题名
   */
  override def writeLocal2Kafka(data: Seq[String],topic: String): Unit ={
   // val kafkaConfig:ProducerConfig = new ProducerConfig(props)
    val producer = new KafkaProducer[String,String](props)
    data.foreach(eachData=>{
      val msg = new ProducerRecord[String,String](topic,Math.random()+"",eachData)
      producer.send(msg)
    })
    producer.close()
  }

  /**
   * 将单String传入到kafka
   * @param data   String类型数据
   * @param topic   主题名
   */
  override def writrString2Kafka(data: String,topic: String): Unit ={
    //val kafkaConfig:ProducerConfig = new ProducerConfig(props)
    val producer = new KafkaProducer[String,String](props)
    val msg = new ProducerRecord[String,String](topic,Math.random()+"",data)

    producer.send(msg)
    producer.close()
  }

  override def writeData2KafkaByKey(data: String,topic: String,key:String): Unit ={
   // val kafkaConfig:ProducerConfig = new ProducerConfig(props)
    val producer = new KafkaProducer[String,String](props)
    val msg = new ProducerRecord[String,String](topic,key,data)

    producer.send(msg)
    producer.close()
  }

}
