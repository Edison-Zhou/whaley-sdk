package cn.whaley.sdk.algorithm

/**
 * Created by mycomputer on 1/11/2016.
 */
case class WordPattern(s:String, table:Array[Int],l:Int)
class KMP(userWords: Array[String]) extends Serializable{
  val tableArray = userWords.map(x=>WordPattern(x,makeTable(x),x.length))
  val len = tableArray.length
  private def makeTable(pattern: String): Array[Int] = {
    val patternLength = pattern.length
    require(patternLength > 0)
    def inner(i: Int, j: Int, table: Array[Int] = Array(-1, 0)): Array[Int] = {
      if (j == patternLength - 1) table
      else {
        val isMatch = pattern(i) == pattern(j)
        val index = if (isMatch) table.last + 1 else 0
        inner(if (isMatch) i + 1 else i, j + 1, table :+ index)
      }
    }
    if (patternLength == 1) Array(-1)
    else inner(0, 1)
  }

  def search(text: String,w:WordPattern): Boolean = {
    if (w.l <= 0) return true
    else {
      val textLength = text.length
      if (textLength < w.l) false
      else {
        def comp(i: Int, j: Int): Boolean = {
          if (textLength - i < w.l - j)
            false
          else {
            (i < textLength, j < w.l) match {
              case (true, true) =>
                if (text(i) == w.s(j)) {
                  comp(i + 1, j + 1)
                } else {
                  val index = w.table(j)
                  val (_i, _j) = if (index == -1) (i + 1, 0) else (i, index)
                  comp(_i, _j)
                }
              case (false, true) => false
              case _ => true
            }
          }
        }
        comp(0, 0)
      }
    }
  }

  def run(a:Array[String]):Array[Boolean]={
    a.map(s=>{
      var notContain=true
      var i = 0
      while(i<len&&notContain){

        if(search(s,tableArray(i))) notContain=false
        i+=1
      }
      !notContain
    })
  }

  def searchIndex(text:String):Int={
    var i = 0
    var notContain = true
    while(i<len&&notContain){
      if(search(text,tableArray(i))){
        notContain =false
      }else i+=1
    }

    if(!notContain) i else -1
  }
  def searchIndex(a:Array[String]):Array[Int]={
    a.map(x=>searchIndex(x))
  }

}
