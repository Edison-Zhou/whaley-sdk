package cn.whaley.sdk.dataOps

import cn.whaley.sdk.customize.TotalConf
import cn.whaley.sdk.dataopsio.RedisOpsIO
import cn.whaley.sdk.utils.CodeIDOperator
import org.json.JSONObject
import redis.clients.jedis._
import java.util

import scala.collection.mutable.Map

/**
 * Created by xutong on 2016/3/11.
 * redis工具类
 */
class RedisOps(redisName: String) extends Serializable with RedisOpsIO{

  /**
   * 定义ProgramRedis使用的一些常量
   */
  lazy private val TITLE = "title"
  lazy private val AREA = "area"
  lazy private val CONTENT_TYPE = "contentType"
  lazy private val SUPPLY_TYPE = "supply_type"

  /**
   * 获取redis数据库参数
   */
  val prop = TotalConf.setRedisConf(redisName)
  val metadata_host=prop.getProperty("metadata_host")
  val metadata_port=prop.getProperty("metadata_port").toInt
  val metadata_db=prop.getProperty("metadata_db").toInt

  /**
   * 初始化Jedis
   */
  lazy val config:JedisPoolConfig = getJedisPoolConfig()
  lazy val metadataPool: JedisPool=new JedisPool(config ,metadata_host, metadata_port,100*Protocol.DEFAULT_TIMEOUT,null,metadata_db)
  lazy val metadata_jedis: Jedis = metadataPool.getResource


  /**
   * 初始化jedisConfig参数
   * @return
   */
  override def getJedisPoolConfig() = {
    val config = new JedisPoolConfig()
    config.setMaxWaitMillis(10000)
    config.setMaxTotal(100)
    config
  }


  /**
   * 返回jedisPool
   * @return
   */
  def getJedisPool()={
    metadataPool
  }

  /**
   * 返回jedis
   * @return
   */
  def getJedis() ={
    metadata_jedis
  }

  /**
   * 根据key查询redis数据
   * @param key 键值
   * @return  查询数据
   */
  def getDataByKey(key: String) = {
    metadata_jedis.get(key)
  }

/*  /**
   * 将数据插入到jedis中
   * @param key  键值key
   * @param value  数据值value
   * @return
   */
  def addDataByKey(key: String,value: String*)={
    metadata_jedis.sadd(key,value.toSeq)
  }*/

  /**
   * 返回数据库中key所对应的基数
   * @param key  键值key
   * @return     基数值
   */
  def getNumByKey(key: String)={
    metadata_jedis.zcard(key)
  }



  /**
   * Function：设置最大等待时间
   * @param time 时长
   */
  def setWaitTime(time:Long): Unit =
  {
    config.setMaxWaitMillis(time)
  }

  /**
   * Function: 设置最大数量
   * @param num 数量
   */
  def setMaxTotal(num:Int): Unit ={
    config.setMaxTotal(num)
  }

  /**
   * Function:根据节目sid返回节目title
   * @param sid
   * @return
   */
  def getTitleBySid(sid:String)={
    if(metadata_jedis == null){
      this
    }
    val metadata = metadata_jedis.get(sid)
    var title = sid
    if(metadata != null && metadata != "nil") {
      val jsonObject =new JSONObject(metadata)
      title=jsonObject.getJSONArray(TITLE).get(0).toString()
      if (title != null) {
        title = title.replace("'", "");
        title = title.replace("\t", " ")
        title = title.replace("\r", "-")
        title = title.replace("\n", "-")
        title = title.replace("\r\n", "-")
      }else title = sid
    }
    title
  }

  /**
   *
   * @param sidArray sid的数组
   * @return title的数组
   */
  def getTitleBySidArray(sidArray: Array[String])={

    val num = sidArray.size
    val titleArray = new Array[String](num)
    for(i<- 0 until num){
      titleArray(i) = getTitleBySid(sidArray(i))
    }
    titleArray
  }

  /**
   * Function:根据节目sid返回节目地区
   * @param sid
   * @return
   */
  def getAreaBySid(sid:String)={
    if(metadata_jedis == null){
      this
    }
    val metadata = metadata_jedis.get(sid)
    var area:String = null
    if(metadata != null && metadata != "nil") {
      val jsonObject =new JSONObject(metadata)
      area=jsonObject.getJSONArray(AREA).get(0).toString()
    }
    area
  }

  /**
   *
   * @param sidArray sid的数组
   * @return area的数组
   */
  def getAreaBySidArray(sidArray: Array[String])={

    val num = sidArray.size
    val areaArray = new Array[String](num)
    for(i<- 0 until num){
      areaArray(i) = getAreaBySid(sidArray(i))
    }
    areaArray
  }

  /**
   * 根据sid查询contentType
   * @param sid
   * @return
   */
  def getContentTypeBySid(sid: String)={
    if(metadata_jedis == null){
      this
    }
    val metadata = metadata_jedis.get(sid)
    var contentType:String = null
    if(metadata != null && metadata != "nil") {
      val jsonObject =new JSONObject(metadata)
      contentType=jsonObject.getJSONArray(CONTENT_TYPE).get(0).toString()
    }
    contentType
  }

  /**
   *
   * @param sidArray sid的数组
   * @return contentType的数组
   */
  def getContentTypeBySidArray(sidArray: Array[String])={

    val num = sidArray.size
    val contentTypeArray = new Array[String](num)
    for(i<- 0 until num){
      contentTypeArray(i) = getContentTypeBySid(sidArray(i))
    }
    contentTypeArray
  }

  /**
   * Function:根据节目sid 返回节目vip属性
   * @param sid
   * @return
   */
  def getVIPBySid(sid:String)={
    if(metadata_jedis == null){
      this
    }
    val metadata = metadata_jedis.get(sid)
    var vip:String = "free"
    if(metadata != null && metadata != "nil") {
      val jsonObject =new JSONObject(metadata)
      vip=jsonObject.getJSONArray(SUPPLY_TYPE).get(0).toString()
    }
    vip
  }

  /**
   *
   * @param sidArray sid的数组
   * @return vip的数组
   */
  def getVIPBySidArray(sidArray: Array[String])={

    val num = sidArray.size
    val VIPArray = new Array[String](num)
    for(i<- 0 until num){
      VIPArray(i) = getTitleBySid(sidArray(i))
    }
    VIPArray
  }

  /**
   * Function:get program fields by program sid
   * @param sid
   * @param fieldNames
   * @return
   */
  def getFieldValues(sid:String, fieldNames:String*)={
    if(metadata_jedis == null){
      this
    }
    val metadata = metadata_jedis.get(sid)
    val map = Map[String,String]()
    if(metadata != null && metadata != "nil"){
      val jsonObject =new JSONObject(metadata)
      fieldNames.foreach(x =>{
        val jsonArray = jsonObject.getJSONArray(x)
        var fieldValue:String = null
        if(jsonArray != null && jsonArray.length() > 0){
          fieldValue = jsonArray.get(0).toString()
        }
        map += (x -> fieldValue)
      })
    }
    if(map.size > 0)
      map
    else null
  }

  /**
   * 根据id查询title
   * @param id
   * @return
   */
  def getTitleByID(id: Int)={
    val sid = CodeIDOperator.idToCode(id)
    getTitleBySid(sid)
  }

  /**
   *
   * @param idArray
   * @return
   */
  def getTitleByIDArray(idArray: Array[Int])={
    val num = idArray.size
    val titleArray = new Array[String](num)
    for(i<- 0 until num){
      var sid = CodeIDOperator.idToCode(idArray(i))
      titleArray(i) = getTitleBySid(sid)
    }
    titleArray
  }

  /**
   * 根据id查询VIP
   * @param id
   * @return
   */
  def getVIPByID(id: Int)={
    val sid = CodeIDOperator.idToCode(id)
    getVIPBySid(sid)
  }

  /**
   *
   * @param idArray
   * @return
   */
  def getVIPByIDArray(idArray: Array[Int])={
    val num = idArray.size
    val VIPArray = new Array[String](num)
    for(i<- 0 until num){
      var sid = CodeIDOperator.idToCode(idArray(i))
      VIPArray(i) = getVIPBySid(sid)
    }
    VIPArray
  }

  /**
   * 根据id查询contenttype
   * @param id
   * @return
   */
  def getContentTypePByID(id: Int)={
    val sid = CodeIDOperator.idToCode(id)
    getContentTypeBySid(sid)
  }

  /**
   *
   * @param idArray
   * @return
   */
  def getContentTypeByIDArray(idArray: Array[Int])={
    val num = idArray.size
    val contentTypeArray = new Array[String](num)
    for(i<- 0 until num){
      var sid = CodeIDOperator.idToCode(idArray(i))
      contentTypeArray(i) = getContentTypeBySid(sid)
    }
    contentTypeArray
  }

  /**
   * get program fields by id
   * @param id
   * @param fieldNames
   * @return
   */
  def getFieldValuesByID(id: Int, fieldNames:String*)={
    val sid = CodeIDOperator.idToCode(id)
    if(metadata_jedis == null){
      this
    }
    val metadata = metadata_jedis.get(sid)
    val map = Map[String,String]()
    if(metadata != null && metadata != "nil"){
      val jsonObject =new JSONObject(metadata)
      fieldNames.foreach(x =>{
        val jsonArray = jsonObject.getJSONArray(x)
        var fieldValue:String = null
        if(jsonArray != null && jsonArray.length() > 0){
          fieldValue = jsonArray.get(0).toString()
        }
        map += (x -> fieldValue)
      })
    }
    if(map.size > 0)
      map
    else null
  }

  /**
   * 推荐返回redis集群节点
   * @return
   */
  def getJedisClusterNodes()={
    RedisOps.getJedisClusterNodes()
  }

  /**
   * close jedis
   */
  def destroy(){
    metadataPool.returnResource(metadata_jedis);
    metadataPool.destroy();
  }
}

object RedisOps{

  def getJedisClusterNodes()={
    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort("10.10.2.21",6379))
    jedisClusterNodes.add(new HostAndPort("10.10.2.20",6379))
    jedisClusterNodes.add(new HostAndPort("10.10.2.19",6379))
    jedisClusterNodes
  }
}
