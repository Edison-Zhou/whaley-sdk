package cn.whaley.sdk.udf

/**
 * Created by Administrator on 2016/8/1.
 */
object GetLengthFromString {

  def getLength(value: String)={
    if(value == null){
      0
    }else{
      value.length
    }

  }
}
