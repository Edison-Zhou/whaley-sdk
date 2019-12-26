package cn.whaley.sdk.dataOps

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.{List, Map}

import cn.whaley.sdk.customize.TotalConf
import cn.whaley.sdk.dataopsio.MySqlOpsIO
import org.apache.commons.dbutils.handlers.{ArrayHandler, ArrayListHandler, MapListHandler}
import org.apache.commons.dbutils.{DbUtils, QueryRunner}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

import scala.collection.JavaConversions._
import java.lang.{Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}

import cn.whaley.sdk.vo.Row

import scala.reflect.ClassTag
import MySqlOps._

/**
 * Created by xutong on 2016/3/11.
 * mysql的工具类
 */
class MySqlOps(DBName: String) extends Serializable with MySqlOpsIO{

  /**
   * 定义变量
   */
  private var conn: Connection = null
  private var queryRunner: QueryRunner = null

  val prop = TotalConf.setDBConf(DBName)
  val driver = prop.getProperty("driver")
  val url = prop.getProperty("url")
  val user = prop.getProperty("user")
  val password = prop.getProperty("password")

  try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,user,password)
      queryRunner = new QueryRunner
    }catch{
      case e: Exception=>{
        e.printStackTrace()
      }
    }

  /**
   * 获取JdbcRDD
    *
    * @param sc
   * @param sql  查询语句
   * @param table  表名
   * @param func   结果函数
   * @tparam T
   * @return
   */
  override def getJdbcRDD[T: ClassTag](sc: SparkContext,sql: String,table: String,func:(ResultSet=>T),numPartition: Int = 5):JdbcRDD[T]={

    MySqlOps.getJdbcRDD[T](sc,sql,table,func,driver,url,user,password,queryMaxMinID(table),numPartition)
  }

  /**
   * 根据sql建立JdbcRDD
    *
    * @param sc
   * @param sql  查询语句
   * @param IDsql   查询站位符的sql语句
   * @param func    输出函数
   * @tparam T
   * @return
   */
   override def getJdbcRDDBySql[T: ClassTag](sc: SparkContext,sql: String,IDsql: String,func:(ResultSet=>T),numPartition: Int = 5):JdbcRDD[T]={

    MySqlOps.getJdbcRDDBySql(sc,sql,func,driver,url,user,password,queryMaxMinIDBySql(IDsql),numPartition)
  }
/**
   * 向数据库中插入记录
  *
  * @param sql 预编译的sql语句
   * @param params 插入的参数
   * @return 影响的行数
   * @throws SQLException
   */
   override def insert(sql: String, params: Any*): Int = {
    queryRunner.update(conn, sql, asScala(params): _*)
  }

  /**
   * 通过指定的SQL语句和参数查询数据
    *
    * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  override def selectOne(sql: String, params: Any*):Array[AnyRef] = {
    queryRunner.query(conn, sql, new ArrayHandler(), asScala(params): _*)
  }

  /**
   * 通过指定的SQL语句和参数查询数据
    *
    * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  override def selectMapList(sql: String, params: Any*): List[Map[String,AnyRef]] = {
    queryRunner.query(conn, sql, new MapListHandler(), asScala(params): _*)
  }

  /**
   * 通过指定的SQL语句和参数查询数据
    *
    * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  override def selectArrayList(sql: String, params: Any*): List[Array[AnyRef]] = {
    queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
  }

  /**
   * 删除错乱的数据
    *
    * @param sql delete sql
   * @param params delete sql params
   * @return
   */
  override def delete(sql: String,params: Any*) = {
    queryRunner.update(conn, sql,asScala(params): _*)
  }

  /**
   * 查询ID号
    *
    * @param column　列名
   * @param tableName 表名
   * @return  最大最小ID
   */
  override def queryMaxMinID(tableName: String,column: String = "id" ):(Long,Long)={
    val sql = s"select MIN($column),MAX($column) from $tableName"
    val statement = conn.createStatement()
    val result =  statement.executeQuery(sql)
    result.next()
    val minID = result.getLong(1)
    val maxID = result.getLong(2)
    result.close()
    (minID,maxID)
  }

  /**
   * 查询ID号
    *
    * @param sql　查询语句
   * @return  最大最小ID
   */
  override def queryMaxMinIDBySql(sql: String ):(Long,Long)={
    val statement = conn.createStatement()
    val result =  statement.executeQuery(sql)
    result.next()
    val minID = result.getLong(1)
    val maxID = result.getLong(2)
    result.close()
    (minID,maxID)
  }
  /**
   * 释放资源，如关闭数据库连接
   */
  override def destory() {
    DbUtils.closeQuietly(conn)
  }

  override def select(sql: String, params: Any*): Seq[Row] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(Row(_))
  }

  override def select[T](sql: String, params: Any*)(op: (Row) => T): Seq[T] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => op(Row(x)))
  }

  override def select2Tuple2[T1, T2](sql: String, params: Any*): Seq[(T1, T2)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1))
    })
  }

  override def select2Tuple3[T1, T2, T3](sql: String, params: Any*): Seq[(T1, T2, T3)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2))
    })
  }

  override def select2Tuple4[T1, T2, T3, T4](sql: String, params: Any*): Seq[(T1, T2, T3, T4)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3))
    })
  }

  override def select2Tuple5[T1, T2, T3, T4, T5](sql: String, params: Any*): Seq[(T1, T2, T3, T4, T5)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3),row.getAs[T5](4))
    })
  }

  override def select2Tuple6[T1, T2, T3, T4, T5, T6](sql: String, params: Any*): Seq[(T1, T2, T3, T4, T5, T6)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3),row.getAs[T5](4),row.getAs[T6](5))
    })
  }

  override def select2Tuple7[T1, T2, T3, T4, T5, T6, T7](sql: String, params: Any*): Seq[(T1, T2, T3, T4, T5, T6, T7)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3),row.getAs[T5](4),row.getAs[T6](5),row.getAs[T7](6))
    })
  }

  override def select2Tuple8[T1, T2, T3, T4, T5, T6, T7, T8](sql: String, params: Any*): Seq[(T1, T2, T3, T4, T5, T6, T7, T8)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3),row.getAs[T5](4),row.getAs[T6](5),row.getAs[T7](6),row.getAs[T8](7))
    })
  }

  override def select2Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9](sql: String, params: Any*): Seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3),row.getAs[T5](4),row.getAs[T6](5),row.getAs[T7](6),row.getAs[T8](7),row.getAs[T9](8))
    })
  }

  override def select2Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](sql: String, params: Any*): Seq[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] = {
    val resultSet = queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
    resultSet.map(x => {
      val row = Row(x)
      (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3),row.getAs[T5](4),row.getAs[T6](5),row.getAs[T7](6),row.getAs[T8](7),row.getAs[T9](8),row.getAs[T10](9))
    })
  }

  override def update(sql: String, params: Any*): Int = {
    queryRunner.update(conn, sql,asScala(params): _*)
  }
}

object MySqlOps{
  def getJdbcRDD[T: ClassTag](sc: SparkContext,sql: String,table: String,func:(ResultSet=>T),driver:String,url:String,user:String,password:String,maxMinID:(Long,Long),numPartition: Int):JdbcRDD[T]={

    new JdbcRDD[T](sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,user,password)
    },
      sql,
      maxMinID._1,
      maxMinID._2,
      numPartition,
      func)
  }

  def getJdbcRDDBySql[T: ClassTag](sc: SparkContext,sql: String,func:(ResultSet=>T),driver:String,url:String,user:String,password:String,maxMinID:(Long,Long),numPartition: Int):JdbcRDD[T]={

    new JdbcRDD[T](sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,user,password)
    },
      sql,
      maxMinID._1,
      maxMinID._2,
      numPartition,
      func)
  }

  def asScala(params: Seq[Any]) = {
    params.map{
      case null => null
      case e:Long => new JLong(e)
      case e:Double => new JDouble(e)
      case e:String => e
      case e:Short => new JShort(e)
      case e:Int => new Integer(e)
      case e:Float => new JFloat(e)
      case e:Any => e.asInstanceOf[Object]
    }
  }
}
