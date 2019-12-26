package cn.whaley.sdk.utils

/**
 * userID转换程序
 */
object UserIDOperator {
  def myHashCode(s:String):Long={
    var hash:Long = 0
    for(i<- 0 until s.length){
      hash = 47*hash + s.charAt(i).toLong
    }
    hash
  }

  /*def userId2Long(s:String) = {
    var hash:Long = 0
    for(i<- 0 until s.length){
      hash = 47*hash + s.charAt(i).toLong
    }


    if(hash<1000000000){
      hash = hash + 1000000000*(hash+"").charAt(0).toInt
    }
    hash
  }*/

  def userId2Long(s:String) = {
    var hash:Long = 0
    for(i<- 0 until s.length){
      hash = 47*hash + s.charAt(i).toLong
    }

    hash = math.abs(hash)

    if(hash<1000000000){
      hash = hash + 1000000000*(hash+"").charAt(0).toInt
    }
    hash
  }

  implicit class StringOperator(uid:String){
    def longHashCode() = myHashCode(uid)
    def absHashCode() = math.abs(myHashCode(uid))
    def hybridHashCode() = userId2Long(uid)
  }

}

