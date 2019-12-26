package cn.whaley.sdk.udf

import cn.whaley.sdk.utils.HttpUtils
import org.json.JSONObject
/**
 * Created by Administrator on 2016/6/12.
 */
object MetisUdf {

  var mv_path_code_name_map:Map[String,Map[String,String]] = Map()

  def getNameByThirdPathCode(contentType:String,code:String)={
    if(mv_path_code_name_map.size == 0){
      val url = "http://vod.aginomoto.com/Service/TreeSite?code=mv_site"
      getCodeToNameMap(url)
    }
    val codeToName = mv_path_code_name_map.getOrElse(contentType,null)
    var result = ""
    if (codeToName != null && codeToName.size > 0){
      result = codeToName.getOrElse(code,"")
    }
    result
  }

  /**
   * translate JsonObject to Map
   */
  def getCodeToNameMap(url:String)={
    val json = HttpUtils.get(url)
    val jsonObj = new JSONObject(json)
    val jsonArray = jsonObj.getJSONArray("children")
    for(i <- 0 until jsonArray.length()){
      val arr:JSONObject = jsonArray.getJSONObject(i)
      val contentType = arr.getString("code")
      val children = arr.getJSONArray("children")
      var child_code_name_map:Map[String,String] = Map()
      child_code_name_map += (contentType -> arr.getString("name"))
      for(j <- 0 until children.length()){
        val child:JSONObject = children.getJSONObject(j)
        child_code_name_map += (child.getString("code") -> child.getString("name"))
      }
      mv_path_code_name_map += (contentType -> child_code_name_map)
    }
    mv_path_code_name_map
  }


  /**
   * metis mv sub scan class to name
   */
  def mvSubScanClassToName(contentType:String,code:String)={
    getNameByThirdPathCode(contentType,code)

  }

  /**
   * metis mv sub play class to name
   */
  def mvSubPlayClassToName(path:String)={
    if(path !=null && path != ""){
      val reg = "class-(\\w+)-(\\w+)".r
      val format = reg.findFirstMatchIn(path)
      format match {
        case Some(x) =>{
          getNameByThirdPathCode(x.group(1),x.group(2))
        }
        case None => ""
      }
    }else
      ""
  }

  /**
   * metis mv sub play class to name
   */
  def mvSubPlaySiteConcertToName(path:String)={
    if(path !=null && path != ""){
      val reg = "site_concert-(\\w+)".r
      val format = reg.findFirstMatchIn(path)
      format match {
        case Some(x) =>{
          getNameByThirdPathCode("site_concert",x.group(1))
        }
        case None => ""
      }
    }else
      ""
  }

  def metisInterviewType(s:String)={
    val interviewType = Map("discover"->"发现","dailyPaper"->"日报","mine"->"我的",
      "recommend"->"推荐","review"->"影评","knowledge"->"电影知识","adventures"->"猎奇",
      "character"->"人物","interest"->"趣味")
    interviewType.getOrElse(s,"")
  }


  /**
   * metis mv scan class to name
   */
  def mvScanClassToName(s:String)={
    val mvClassToName = Map("site_mvstyle"->"风格","site_mvarea"->"地区","site_mvyear"->"年代")
    mvClassToName.getOrElse(s,"")
  }

  /**
   * metis mv play class to name
   */
  def mvPlayClassToName(path:String)={
    if(path !=null && path != ""){
      val classToName = Map("site_mvstyle"->"风格","site_mvarea"->"地区","site_mvyear"->"年代")
      val reg = "class-(\\w+)".r
      val format = reg.findFirstMatchIn(path)
      format match {
        case Some(x) =>{
          val rankCode = x.group(1)
          classToName.getOrElse(rankCode,"")
        }
        case None => ""
      }
    }else
      ""
  }




}
