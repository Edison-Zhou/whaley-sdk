package cn.whaley.sdk.udf

import java.sql.{ResultSet, Statement, DriverManager, Connection}

import cn.whaley.sdk.dataOps.MySqlOps

import scala.collection.mutable.Map

/**
 * Created by Administrator on 2016/6/6.
 */
object GetInfoFromMySQL {

  /**
   * initialize map
   */
  var sidToVIPMap:Map[String,String ] = Map()
  var subjectNameMap:Map[String,String ] = Map()
  var sportLiveNameMap:Map[String,String ] = Map()
  var sportLiveProgramMap:Map[String,String ] = Map()

  /**
   * initialize sql
   */
  val sidToVIPMapSql:String = "SELECT sid,supply_type FROM tvservice.mtv_program"
  val subjectNameMapSql:String = "SELECT code,name FROM tvservice.mtv_subject"
  val sportLiveNameMapSql:String = "SELECT pid,title FROM mtv_cms.sailfish_sport_match"
  val sportLiveProgramMapSql:String = "SELECT sid,title FROM mtv_cms.sailfish_sport_match"



  /**
   * Function: initialize map
   * @param name
   * @param sql
   * @param map
   */
  def initSidMap(name: String, sql: String, map: Map[String, String]) ={
    try {
      val mySqlDb = new MySqlOps(name)
      Class.forName(mySqlDb.driver)
      val conn: Connection = DriverManager.getConnection(mySqlDb.url, mySqlDb.user, mySqlDb.password)
      val stat: Statement = conn.createStatement
      val rs: ResultSet = stat.executeQuery(sql)
      while (rs.next) {
        map += (rs.getString(1) -> rs.getString(2))
      }
      rs.close
      stat.close
      conn.close
      mySqlDb.destory()
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(e)
      }
    }
  }

  /**
   * Function:obtain subject name according to code
   * @param code
   * @return
   */
  def getSubjectNameByCode(code: String):String ={
    if(subjectNameMap.size == 0){
      initSidMap("helios_recommend_mysql",subjectNameMapSql,subjectNameMap)
    }
    subjectNameMap.getOrElse(code,null)
  }

  /**
   * Function:obtain sport live name according to pid
   * @param pid
   * @return
   */
  def getSportLiveNameByPid(pid: String):String ={
    if(sportLiveNameMap.size == 0){
      initSidMap("bi_mtv_mysql", sportLiveNameMapSql, sportLiveNameMap)
    }
    sportLiveNameMap.getOrElse(pid,pid)
  }

  /**
   * Function:obtain sport live name according to pid
   * @param sid
   * @return
   */
  def getSportLiveProgramBySid(sid: String):String ={
    if(sportLiveProgramMap.size == 0){
      initSidMap("bi_mtv_mysql", sportLiveProgramMapSql, sportLiveProgramMap)
    }
    sportLiveProgramMap.getOrElse(sid,sid)
  }

  def getSportLiveInfo(sid: String):String={
    val info = sid.split("-")
    if(1 == info.length)
      getSportLiveNameByPid(sid)
    else if(2 == info.length)
      getSportLiveProgramBySid(info(1))
    else
      sid
  }
}
