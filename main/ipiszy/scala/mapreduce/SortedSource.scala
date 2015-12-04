package ipiszy.scala.mapreduce

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by carolzhang on 12/3/15.
  */
class SortedSource(val sources: Array[Source]) extends Source {
  private val queue =
    new mutable.PriorityQueue[Tuple2[Tuple2[Array[Byte], Array[Byte]], Int]]()(
      new Ordering[Tuple2[Tuple2[Array[Byte], Array[Byte]], Int]]{
        override def compare(x: ((Array[Byte], Array[Byte]), Int),
                             y: ((Array[Byte], Array[Byte]), Int)): Int = {
          Ordering.Iterable[Byte].compare(x._1._1, y._1._1)
        }
      }
    )

  override def readNextKVs(): (Array[Byte], Array[Array[Byte]]) = {
    if (queue.isEmpty) null
    else {
      val result = new ArrayBuffer[Array[Byte]]()
      val key = queue.head._1._1
      while (!queue.isEmpty && key.equals(queue.head._1._1)) {
        result += queue.head._1._2
        val idx = queue.head._2
        queue.dequeue()
        val next = sources(idx).readNextKV()
        if (next != null) queue.enqueue((next, idx))
      }
      (key, result.toArray)
    }
  }
}
