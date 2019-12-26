package cn.whaley.sdk.vo.impl

import cn.whaley.sdk.vo.Row

/**
  * Created by Will on 2016/5/22.
  */
class GenericRow(val values:Array[Any]) extends Row{
  /** Number of elements in the Row. */
  override def length: Int = values.length

  /**
    * Returns the value at position i. If the value is null, null is returned. The following
    * is a mapping between Spark SQL types and return types:
    *
    */
  override def get(i: Int): Any = values(i)
}
