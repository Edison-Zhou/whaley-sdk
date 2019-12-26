package cn.whaley.sdk.algorithm

import java.lang.System.arraycopy
import java.util.{Arrays => JArrays}

import scala.collection.mutable.ArraySeq

/**
 * Utility to select top N items from a collection.
 * Given a collection of M items, the aim is to select top N (as defined by some explicit or implicit Ordering).
 * Basically same as collection.sorted(ordering).take(N).
 * The point of this class is to avoid sorting entire collection of M items while producing same results.
 * Implementation assumes that N is relatively small.
 * {{{
 * scala> import com.gilt.gfc.collection.TopN
 * scala> import scala.math.Ordering.Int
 * scala> val elements = scala.util.Random.shuffle((1 to 10))
 * scala> val topN = TopN[Int](5, elements)
 * topN: Seq[Int] = ArraySeq(1, 2, 3, 4, 5)
 * }}}
 */
object TopN {
  /** Gets the top N items from a given collection. */
  def apply[T](n: Int,
               items: TraversableOnce[T])
              (implicit ord: Ordering[T]): Seq[T] = {
    require(items != null, "items must not be null")
    val topN = TopN(n)(ord)
    topN.addAll(items)
    topN.toSeq
  }

  /** Creates TopN object so that items can be added one at a time. */
  def apply[T](n: Int)
              (implicit ord: Ordering[T]): TopN[T] = {
    require(n > 0, "n must be > 0")
    require(ord != null, "ord must not be null")
    new TopN(n, new ArraySeq[T](n), ord)
  }

}

//
// Not to be constructed directly.
//
// The ugly implementation part, roughly
//   -- sets up an array of N items
//   -- simply appends to an array while total < N
//   -- array is sorted when total == N
//   -- new items are compared against smallest
//      -- nothing to do if less than smallest
//      -- binary search, insert, shift the rest if new item is greater than smallest
//
//  Implementation assumes that N is small and arraycopy on array of N is very fast.
//  It prefers element collections where results are closer to the beginning.
//
final class TopN[T] private(n: Int,
                            topItems: ArraySeq[T], // max .. min
                            ord: Ordering[T]) {
  private val arrayComparator = ord.asInstanceOf[Ordering[Any]]
  private val endIdx = n-1
  private var appendIdx = 0

  /** Adds a single element. */
  final def add[TT <: T](x: TT) {
    if (appendIdx > endIdx) {
      if (arrayComparator.compare(x.asInstanceOf[AnyRef], topItems.array(endIdx)) < 0) { insert(x) }
    } else {
      topItems.array(appendIdx) = x.asInstanceOf[AnyRef]
      if (appendIdx == endIdx) {
        JArrays.sort(topItems.array, arrayComparator)
      }
      appendIdx += 1
    }
  }

  /** Adds all elements of a given collection. */
  final def addAll[TT <: T](xs: TraversableOnce[TT]) {
    xs.foreach(this.add(_))
  }

  /** Generates the resulting top N elements. */
  final def toSeq(): Seq[T] = {
    if (appendIdx < n) {
      val items = topItems.slice(0, appendIdx)
      JArrays.sort(items.array, arrayComparator)
      items
    } else {
      topItems
    }
  }

  final private[this] def insert(x: T) {
    val insertIdx = JArrays.binarySearch(topItems.array, x, arrayComparator)

    val insPoint = if (insertIdx >= 0) {
      insertIdx
    } else {
      -(insertIdx+1)
    }

    if (insPoint < endIdx) {
      arraycopy(topItems.array, insPoint, topItems.array, insPoint+1, (endIdx - insPoint))
    }
    if (insPoint < n) {
      topItems.array(insPoint) = x.asInstanceOf[AnyRef]
    }
  }
}

