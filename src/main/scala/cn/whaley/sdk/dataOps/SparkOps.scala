package cn.whaley.sdk.dataOps

import cn.whaley.sdk.customize.TotalConf
import cn.whaley.sdk.dataopsio.SparkOpsIO
import cn.whaley.sdk.udf._
import com.moretv.bi.util.ProgramRedisUtil
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xutong on 2016/3/15.
 * spark的工具类
 */
object SparkOps extends SparkOpsIO{

  var config = TotalConf.setSparkConf

  var sc = new SparkContext(config)

  /**
   * 获取默认SparkConf
   * @return SparkConf
   */
  override def getSparkConf:SparkConf={
    config
  }

  /**
   * 获取SparkContext
   * @return SparkContext
   */
  override def getSparkContext:SparkContext={
    sc
  }

  /**
   * 获取SQLcontext，其注册了udf
   * @param scTmp
   * @return
   */
  override def getSQLContext(scTmp: SparkContext = sc):SQLContext={

    val sqlContext = SQLContext.getOrCreate(scTmp)

    /**
     * udf
     */
    sqlContext.udf.register("metisInterviewType",(s:String) =>MetisUdf.metisInterviewType(s))

    /**
     * metis mv scan class to name
     */
    sqlContext.udf.register("mvScanClassToName",(s:String) =>MetisUdf.mvScanClassToName(s))

    /**
     * metis mv play class to name
     */
    sqlContext.udf.register("mvPlayClassToName",(path:String) =>MetisUdf.mvPlayClassToName(path))

    /**
     * metis mv sub scan class to name
     */
    sqlContext.udf.register("mvSubScanClassToName",(contentType:String,code:String) =>MetisUdf.mvSubScanClassToName(contentType,code))

    /**
     * metis mv sub play class to name
     */
    sqlContext.udf.register("mvSubPlayClassToName",(path:String) =>MetisUdf.mvSubPlayClassToName(path))
    sqlContext.udf.register("mvSubPlaySiteConcertToName",(path:String) =>MetisUdf.mvSubPlaySiteConcertToName(path))


    sqlContext.udf.register("getLengthFromString",(value:String)=>GetLengthFromString.getLength(value))




    sqlContext.udf.register("transformEventInEvaluate",
      (event:String)=>InfoTransform.transformEventInEvaluate(event))

    sqlContext.udf.register("launcherAccessAreaParser",
      (area:String,app:String)=>LauncherAccessAreaParser.launcherAccessAreaParser(area,app))

    sqlContext.udf.register("launcherAccessLocationParser",
      (accessLocation:String,app:String)=>LauncherAccessAreaParser.launcherAccessLocationParser(accessLocation,app))

    sqlContext.udf.register("launcherLocationIndexParser",
      (locationIndex:String)=>LauncherAccessAreaParser.launcherLocationIndexParser(locationIndex))

    sqlContext.udf.register("moretvLauncherAccessLocationParser",
      (area:String,accessLocation:String)=>LauncherAccessAreaParser.moretvLauncherAccessLocationParser(area,accessLocation))

    sqlContext.udf.register("pathParser",(logType:String,path:String,pathType:String,
      outputType:String)=>PathParser.pathParser(logType,path,pathType,outputType))

    sqlContext.udf.register("getMedusaPathDetailInfo",(path:String,subIndex:Int)=>PathParser.getMedusaPathDetailInfo(path,subIndex))

    sqlContext.udf.register("getPathMainInfo",(x:String,index:Int,subIndex:Int)=>PathParser.getPathMainInfo(x,index,subIndex))

    sqlContext.udf.register("getSplitInfo",(path:String,index:Int)=>PathParser.getSplitInfo(path,index))

    sqlContext.udf.register("getSubjectCode",(path:String)=>PathParser.getSubjectCode(path))

    sqlContext.udf.register("getAreaBySid",(sid:String)=>GetInfoFromRedis.getAreaBySid(sid))

    sqlContext.udf.register("getTitleBySid",(sid:String)=>GetInfoFromRedis.getTitleBySid(sid))

    sqlContext.udf.register("getVIPBySid",(sid:String)=>GetInfoFromRedis.getVIPBySid(sid))

    sqlContext.udf.register("getSportLiveInfo",(sid: String)=>GetInfoFromMySQL.getSportLiveInfo(sid))

    sqlContext.udf.register("getSubjectNameByCode",(code: String)=>GetInfoFromMySQL.getSubjectNameByCode(code))

    sqlContext.udf.register("getTitleBySid",(sid:String)=>ProgramRedisUtil.getTitleBySid(sid))
    sqlContext

  }

  /**
   * 更新默认SparkConf
   * @param value 需要设置的SparkConf属性 map格式
   */
  override def setSparkConf(value:Map[String,String]): Unit ={
    value.keys.foreach{
      i=>{
        if(i == "appName")
          config.setAppName(value(i))
        else
          config.set(i,value(i))
      }
    }
  }


  override def getSparkSession(appName: String):SparkSession={
    val spark = SparkSession.builder().appName(appName).getOrCreate()

    /**
     * udf
     */
    spark.udf.register("metisInterviewType",(s:String) =>MetisUdf.metisInterviewType(s))

    /**
     * metis mv scan class to name
     */
    spark.udf.register("mvScanClassToName",(s:String) =>MetisUdf.mvScanClassToName(s))

    /**
     * metis mv play class to name
     */
    spark.udf.register("mvPlayClassToName",(path:String) =>MetisUdf.mvPlayClassToName(path))

    /**
     * metis mv sub scan class to name
     */
    spark.udf.register("mvSubScanClassToName",(contentType:String,code:String) =>MetisUdf.mvSubScanClassToName(contentType,code))

    /**
     * metis mv sub play class to name
     */
    spark.udf.register("mvSubPlayClassToName",(path:String) =>MetisUdf.mvSubPlayClassToName(path))
    spark.udf.register("mvSubPlaySiteConcertToName",(path:String) =>MetisUdf.mvSubPlaySiteConcertToName(path))


    spark.udf.register("getLengthFromString",(value:String)=>GetLengthFromString.getLength(value))




    spark.udf.register("transformEventInEvaluate",
      (event:String)=>InfoTransform.transformEventInEvaluate(event))

    spark.udf.register("launcherAccessAreaParser",
      (area:String,app:String)=>LauncherAccessAreaParser.launcherAccessAreaParser(area,app))

    spark.udf.register("launcherAccessLocationParser",
      (accessLocation:String,app:String)=>LauncherAccessAreaParser.launcherAccessLocationParser(accessLocation,app))

    spark.udf.register("launcherLocationIndexParser",
      (locationIndex:String)=>LauncherAccessAreaParser.launcherLocationIndexParser(locationIndex))

    spark.udf.register("moretvLauncherAccessLocationParser",
      (area:String,accessLocation:String)=>LauncherAccessAreaParser.moretvLauncherAccessLocationParser(area,accessLocation))

    spark.udf.register("pathParser",(logType:String,path:String,pathType:String,
                                          outputType:String)=>PathParser.pathParser(logType,path,pathType,outputType))

    spark.udf.register("getMedusaPathDetailInfo",(path:String,subIndex:Int)=>PathParser.getMedusaPathDetailInfo(path,subIndex))

    spark.udf.register("getPathMainInfo",(x:String,index:Int,subIndex:Int)=>PathParser.getPathMainInfo(x,index,subIndex))

    spark.udf.register("getSplitInfo",(path:String,index:Int)=>PathParser.getSplitInfo(path,index))

    spark.udf.register("getSubjectCode",(path:String)=>PathParser.getSubjectCode(path))

    spark.udf.register("getAreaBySid",(sid:String)=>GetInfoFromRedis.getAreaBySid(sid))

    spark.udf.register("getTitleBySid",(sid:String)=>GetInfoFromRedis.getTitleBySid(sid))

    spark.udf.register("getVIPBySid",(sid:String)=>GetInfoFromRedis.getVIPBySid(sid))

    spark.udf.register("getSportLiveInfo",(sid: String)=>GetInfoFromMySQL.getSportLiveInfo(sid))

    spark.udf.register("getSubjectNameByCode",(code: String)=>GetInfoFromMySQL.getSubjectNameByCode(code))

    spark.udf.register("getTitleBySid",(sid:String)=>ProgramRedisUtil.getTitleBySid(sid))
    spark
  }
}
