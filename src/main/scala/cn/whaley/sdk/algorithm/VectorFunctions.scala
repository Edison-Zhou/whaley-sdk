package cn.whaley.sdk.algorithm

import java.util.Arrays

import com.github.fommil.netlib.BLAS.{getInstance => blas}

/**
 * Created by mycomputer on 1/11/2016.
 */
object VectorFunctions {
  lazy val usingNatives = com.github.fommil.netlib.BLAS.getInstance.getClass.getName != "com.github.fommil.netlib.F2jBLAS"

  /*
  * dense vector product
  * already been carefully optimised, do not change unless necessary
  * */

  def blasDot(a:Array[Double],b:Array[Double]):Double = {
    blas.ddot(a.length,b,0,1,a,0,1)
  }
  def blasDot(a:Array[Float],b:Array[Float]):Float = {
    blas.sdot(a.length,b,0,1,a,0,1)
  }

  def smallDot(a:Array[Double],b:Array[Double]):Double = {
    DenseVectorSupportMethods.smallDotProduct_Double(a, b, a.length)
  }

  def smallDot(a:Array[Float],b:Array[Float]):Float = {
    DenseVectorSupportMethods.smallDotProduct_Float(a,b,a.length)
  }

  def middleDot(a:Array[Double],b:Array[Double]):Double = {
    DenseVectorSupportMethods.dotProduct_Double(a, 0, b, 0, a.length)
  }

  def middleDot(a:Array[Float],b:Array[Float]):Float={
    DenseVectorSupportMethods.dotProduct_Float(a, 0, b, 0, a.length)
  }

  def denseProduct(a:Array[Double],b:Array[Double]):Double = {
    if(a.length<DenseVectorSupportMethods.MAX_SMALL_DOT_PRODUCT_LENGTH){
      DenseVectorSupportMethods.smallDotProduct_Double(a, b, a.length)
    }else{
      if ((a.length <= 300 || !usingNatives)) {
        middleDot(a,b)
      } else  {
        blasDot(a,b)
      }
    }
  }

  def denseProduct(a:Array[Float],b:Array[Float]):Float = {
    if(a.length<DenseVectorSupportMethods.MAX_SMALL_DOT_PRODUCT_LENGTH){
      DenseVectorSupportMethods.smallDotProduct_Float(a, b, a.length)
    }else{
      if ((a.length <= 300 || !usingNatives)) {
        middleDot(a,b)
      } else  {
        blasDot(a,b)
      }
    }
  }

  /*
  * sparse vector product
  * */

  def gallopSearch(objs: Array[Int], fromIndex: Int, toIndex: Int, toFind: Int):Int = {
    if(objs.length == 0) return ~0

    //    if(toIndex - fromIndex <= 16) return linearSearch(objs, fromIndex, toIndex, toFind)

    var low = fromIndex

    var step = 1
    var high = fromIndex + step

    while (high < toIndex && objs(high) < toFind) {
      low = high
      step *= 2
      high = fromIndex + step
    }

    if (high < toIndex && objs(high) == toFind) {
      high
    } else {
      Arrays.binarySearch(objs, low, math.min(high, toIndex), toFind)
    }
  }




  def smallVectors(
                    aIndex:Array[Int],
                    aValue: Array[Double],
                    bIndex:Array[Int],
                    bValue:Array[Double]
                    ): Double = {
    var result = 0.0
    var aoff: Int = 0
    var boff: Int = 0

    while (aoff < aIndex.length && boff < bIndex.length) {
      if (aIndex(aoff) < bIndex(boff))
        aoff += 1
      else if (bIndex(boff) < aIndex(aoff))
        boff += 1
      else {
        result += aValue(aoff) * bValue(boff)
        aoff += 1
        boff += 1
      }
    }
    result
  }

  def smallVectors(a:Array[(Int,Double)],b:Array[(Int,Double)]):Double={
    var result = 0.0
    var aoff: Int = 0
    var boff: Int = 0

    while (aoff < a.length && boff < b.length) {
      if (a(aoff)._1 < b(boff)._1)
        aoff += 1
      else if (b(boff)._1 < b(aoff)._1)
        boff += 1
      else {
        result += a(aoff)._2 * b(boff)._2
        aoff += 1
        boff += 1
      }
    }
    result

  }

  def bigVectors(
                  aIndex:Array[Int],
                  aValue: Array[Double],
                  bIndex:Array[Int],
                  bValue:Array[Double]
                  ): Double = {
    var result = 0.0
    var aoff: Int = 0
    var boff: Int = 0
    while (aoff < aIndex.length) {
      val aind: Int = aIndex(aoff)
      val bMax = math.min(bIndex.length, aind + 1)

      boff = gallopSearch(bIndex, boff, bMax, aind)
      if (boff < 0) {
        boff = ~boff
        if (boff == bIndex.length) {
          aoff = aIndex.length
        } else {
          val bind: Int = bIndex(boff)

          val aMax = math.min(aIndex.length, bind + 1)
          var newAoff: Int = gallopSearch(aIndex, aoff, aMax, bind)
          if (newAoff < 0) {
            newAoff = ~newAoff
            boff += 1
          }
          aoff = newAoff
        }
      } else {

        result += aValue(aoff) * bValue(boff)
        aoff += 1
        boff += 1
      }
    }
    result
  }


/*
* data must be sorted by index!!
*
* */
  def sparseProduct(aIndex:Array[Int],
                    aValue: Array[Double],
                    bIndex:Array[Int],
                    bValue:Array[Double]
                    ): Double = {


    if (bIndex.length < aIndex.length) {
      sparseProduct(bIndex,bValue,aIndex,aValue)
    } else if (aIndex.length == 0) {
      0.0
    } else if (bIndex.length <= 32) {
      smallVectors(aIndex,aValue,bIndex,bValue)
    } else {
      bigVectors(aIndex,aValue, bIndex,bValue)
    }
  }

}
