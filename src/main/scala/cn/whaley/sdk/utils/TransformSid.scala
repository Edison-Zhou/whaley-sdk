package cn.whaley.sdk.utils

/**
 * Created by mycomputer on 3/29/2016.
 * sid和id相互转换
 */
object TransformSid {
  implicit class Transform(sid:String){
    def toVID = CodeIDOperator.codeToId(sid).toInt
  }

  implicit class TransformId(id:Int){
    def toSid = CodeIDOperator.idToCode(id)
  }
}