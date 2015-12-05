package ipiszy.scala.mapreduce

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by carolzhang on 12/3/15.
  */
class SortedSource(val sources: Array[Source]) extends Source {
  private val queue =
    new mutable.PriorityQueue[((Array[Byte], Array[Byte]), Int)]()(
      new Ordering[((Array[Byte], Array[Byte]), Int)]{
        override def compare(x: ((Array[Byte], Array[Byte]), Int),
                             y: ((Array[Byte], Array[Byte]), Int)): Int = {
          Ordering.Iterable[Byte].compare(x._1._1, y._1._1)
        }
      }.reverse
    )
  for (i <- sources.indices) {
    val next = sources(i).readNextKV()
    if (next != null) queue.enqueue((next, i))
  }

  override def readNextKVs(): (Array[Byte], Array[Array[Byte]]) = {
    if (queue.isEmpty) null
    else {
      val values = new ArrayBuffer[Array[Byte]]()
      val key = queue.head._1._1
      while (queue.nonEmpty && java.util.Arrays.equals(key, queue.head._1._1)) {
        values += queue.head._1._2
        val idx = queue.head._2
        queue.dequeue()
        val next = sources(idx).readNextKV()
        if (next != null) queue.enqueue((next, idx))
      }
      (key, values.toArray)
    }
  }
}
