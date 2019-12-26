package cn.whaley.sdk.utils


import org.apache.spark.sql.{SparkSession, SQLContext}
import cn.whaley.sdk.utils.UserIDOperator._
import cn.whaley.sdk.utils.TransformSid._

/**
  * Created by Administrator on 2016/4/20.
  */
object TransformUDF {
  def calcLongUserId(userId:String):Long={
    if(userId==null || userId.length != 32){
      -1l
    }else{
      userId2Long(userId)
    }
  }

  def transformUserId(userId:String,accountId:Long):Long={
    if(accountId==null || accountId==0l){
      calcLongUserId(userId)

    }else{
      accountId
    }
  }

  def transformUserId(userId:String,accountId:String):Long={
    if(accountId==null || accountId.length !=8){
      calcLongUserId(userId)
    }else{
      accountId.toLong
    }
  }


  def transformUserId(userId:String,accountId:Int):Long={
    if(accountId==null||accountId==0){
      calcLongUserId(userId)
    }else{
      accountId.toLong
    }
  }
  def transformUserId(userId:String,accountId:Any):Long={
    if(accountId == null){
      calcLongUserId(userId)
    }else if(accountId.isInstanceOf[java.lang.Long]){
      transformUserId(userId,accountId.asInstanceOf[Long])
    }else if(accountId.getClass == classOf[java.lang.Integer]){
      transformUserId(userId,accountId.asInstanceOf[Int])
    }else if(accountId.isInstanceOf[java.lang.String]){
      transformUserId(userId,accountId.asInstanceOf[String])
    }else {
      calcLongUserId(userId)
    }
  }

  def transferSid(sid:String):Int={
    sid.toVID
  }

  def registerUDF(implicit sqlContext:SQLContext)={
    /*    sqlContext.udf.register("transformUserId",(userId:String,accountId:Long)=>transformUserId(userId,accountId))
        sqlContext.udf.register("transformUserId",(userId:String,accountId:String)=>transformUserId(userId,accountId))
        sqlContext.udf.register("transformUserId",(userId:String,accountId:Int)=>transformUserId(userId,accountId))*/


    sqlContext.udf.register("transformId",(userId:String) => userId.hybridHashCode())

    sqlContext.udf.register("transformUserId",(userId:String,accountId:Any)=>transformUserId(userId,accountId))
    sqlContext.udf.register("transformSid",(sid:String)=>sid.toVID)
  }

  def registerUDFSS(implicit ss:SparkSession)={
    /*    sqlContext.udf.register("transformUserId",(userId:String,accountId:Long)=>transformUserId(userId,accountId))
        sqlContext.udf.register("transformUserId",(userId:String,accountId:String)=>transformUserId(userId,accountId))
        sqlContext.udf.register("transformUserId",(userId:String,accountId:Int)=>transformUserId(userId,accountId))*/


    ss.udf.register("transformId",(userId:String) => userId.hybridHashCode())

    ss.udf.register("transformUserId",(userId:String,accountId:Any)=>transformUserId(userId,accountId))
    ss.udf.register("transformSid",(sid:String)=>sid.toVID)
  }
}

