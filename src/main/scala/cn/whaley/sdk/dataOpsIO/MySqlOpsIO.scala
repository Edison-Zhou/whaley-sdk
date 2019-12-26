package cn.whaley.sdk.dataopsio

import java.sql.ResultSet
import java.util.{List, Map}

import cn.whaley.sdk.vo.Row
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

import scala.reflect.ClassTag

/**
 * Created by xutong on 2016/5/12.
 * mysql接口层
 */
trait MySqlOpsIO {


  def getJdbcRDD[T: ClassTag](sc: SparkContext,sql: String,table: String,func:(ResultSet=>T),numPartition: Int = 5):JdbcRDD[T]

  def getJdbcRDDBySql[T: ClassTag](sc: SparkContext,sql: String,IDsql: String,func:(ResultSet=>T),numPartition: Int = 5):JdbcRDD[T]

  def insert(sql: String, params: Any*): Int

  def update(sql: String, params: Any*): Int

  def select(sql: String, params: Any*):Seq[Row]

  def select[T](sql: String, params: Any*)(op: Row => T):Seq[T]

  def select2Tuple2[T1,T2](sql: String, params: Any*):Seq[(T1,T2)]
  def select2Tuple3[T1,T2,T3](sql: String, params: Any*):Seq[(T1,T2,T3)]
  def select2Tuple4[T1,T2,T3,T4](sql: String, params: Any*):Seq[(T1,T2,T3,T4)]
  def select2Tuple5[T1,T2,T3,T4,T5](sql: String, params: Any*):Seq[(T1,T2,T3,T4,T5)]
  def select2Tuple6[T1,T2,T3,T4,T5,T6](sql: String, params: Any*):Seq[(T1,T2,T3,T4,T5,T6)]
  def select2Tuple7[T1,T2,T3,T4,T5,T6,T7](sql: String, params: Any*):Seq[(T1,T2,T3,T4,T5,T6,T7)]
  def select2Tuple8[T1,T2,T3,T4,T5,T6,T7,T8](sql: String, params: Any*):Seq[(T1,T2,T3,T4,T5,T6,T7,T8)]
  def select2Tuple9[T1,T2,T3,T4,T5,T6,T7,T8,T9](sql: String, params: Any*):Seq[(T1,T2,T3,T4,T5,T6,T7,T8,T9)]
  def select2Tuple10[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10](sql: String, params: Any*):Seq[(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10)]


  def selectOne(sql: String, params: Any*):Array[AnyRef]

  def selectMapList(sql: String, params: Any*): List[Map[String,AnyRef]]

  def selectArrayList(sql: String, params: Any*): List[Array[AnyRef]]

  def delete(sql: String,params: Any*)

  def queryMaxMinID(tableName: String,column: String = "id" ):(Long,Long)

  def queryMaxMinIDBySql(sql: String ):(Long,Long)

  def destory()


}
