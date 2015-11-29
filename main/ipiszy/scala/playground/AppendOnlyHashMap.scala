package ipiszy.scala.playground

import java.util.NoSuchElementException

/**
  * Created by carolzhang on 11/27/15.
  * Unresolved issues:
  *   1. All uninitialized variables are null.
  *   2. When to throw exceptions or assert. assert, require, ensuring
  *   3. Use anonymous internal class to write iterator.
  *   4. Iterator.next() should throw exception upon fail.
  *   5. The difference between eq and equals? AnyRef.eq and AnyRef.equals?
  *   6. a % b operation. If b == 2^n, then a % b = a & (b - 1)?
  */

class AppendOnlyHashMap[K, V](initializeSize: Int = 64)
    extends Iterable[(K, V)] with Serializable { outer =>

  require((initializeSize <= AppendOnlyHashMap.MAX_SIZE &&
    initializeSize >= AppendOnlyHashMap.MIN_SIZE),
    "The initial size is either too small or too large. Max size: " +
      AppendOnlyHashMap.MAX_SIZE + ", min size: " + AppendOnlyHashMap.MIN_SIZE +
      ", current initial size: " + initializeSize)

  private var arr: Array[(K, V)] = new Array[(K, V)](initSize(initializeSize))
  private val LOAD_FACTOR = 0.7
  private var curSize = 0
  private var hasNullKey = false
  private var nullValue: V = null.asInstanceOf[V]

  private def initSize(initializeSize: Int): Int = {
    val size = Integer.highestOneBit(initializeSize)
    if (size < initializeSize) size << 1
    else size
  }

  private def getCandidateIdx(key: K, arr: Array[(K, V)]) = {
    var idx = key.hashCode() % arr.length
    var i = 0
    while (arr(idx) != null &&
      !(arr(idx)._1.asInstanceOf[AnyRef].eq(key.asInstanceOf[AnyRef])) &&
      !(arr(idx)._1.equals(key)) && i < arr.length) {
      i += 1
      idx = (idx + i) % arr.length
    }
    assert(arr(idx) == null || arr(idx)._1.equals(key),
      "The key to be put/get(" + key + ") is different with the key(" +
      arr(idx)._1 + ") in the map.")
    idx
  }

  private def putInternal(key: K, value: V, arr: Array[(K, V)]) = {
    val idx = getCandidateIdx(key, arr)
    val isNew = (arr(idx) == null)
    arr(idx) = (key, value)
    isNew
  }

  private def growIfNecessary() = {
    if (curSize > arr.length * LOAD_FACTOR) {
      val nextSize = arr.length << 1
      require(nextSize <= AppendOnlyHashMap.MAX_SIZE,
        "The hashmap contains too many elements. Max capacity: " +
        AppendOnlyHashMap.MAX_SIZE * LOAD_FACTOR +
        ", current number of elements: " + curSize)
      val newArr = new Array[(K, V)](nextSize)
      for ((k, v) <- arr) putInternal(k, v, newArr)
      arr = newArr
    }
  }

  def put(key: K, value: V): Unit = {
    if (key == null) {
      hasNullKey = true
      nullValue = value
    } else if (putInternal(key, value, arr)) {
      curSize += 1
      growIfNecessary()
    }
  }

  def get(key: K): V = {
    if (key == null) {
      if (hasNullKey) nullValue
      else null.asInstanceOf[V]
    } else {
      val idx = getCandidateIdx(key, arr)
      if (arr(idx) == null) null.asInstanceOf[V]
      else arr(idx)._2
    }
  }

  override def size = curSize + (if (hasNullKey) 1 else 0)

  override def iterator: scala.collection.Iterator[(K, V)] = new Iterator[(K, V)] {
    private var idx = -1
    override def hasNext: Boolean = {
      if (idx == -1) {
        if (hasNullKey) return true
        else idx += 1
      }
      while (idx < outer.arr.length) {
        if (outer.arr(idx) != null) return true
        idx += 1
      }
      false
    }
    override def next(): (K, V) = {
      if (hasNext) {
        idx += 1
        if (idx == 0) {
          (null.asInstanceOf[K], nullValue)
        } else {
          outer.arr(idx - 1)
        }
      } else {
        throw new NoSuchElementException("Has reached the end of the hashmap!")
      }
    }
  }
}

object AppendOnlyHashMap {
  val MIN_SIZE = 1
  val MAX_SIZE = 1 << 29
}