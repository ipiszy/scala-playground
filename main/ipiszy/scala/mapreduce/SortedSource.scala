package ipiszy.scala.mapreduce

import scala.collection.mutable

/**
  * Created by carolzhang on 12/3/15.
  */
class SortedSource(val sources: Array[Source]) extends Source {
  private val queue =
    new mutable.PriorityQueue[Tuple2[Tuple2[Array[Byte], Array[Byte]], Int]]()(
      Ordering.by(ele => ele._1._1)
    )
  override def readNextValue(): Array[Byte] = {
    assert(false, "Unsupported method!")
    null
  }

  override def readNextKV(): (Array[Byte], Array[Byte]) = {
    if (queue.isEmpty) null
    else {
      val ele = queue.dequeue()
      val next = sources(ele._2).readNextKV()
      if (next != null) queue.enqueue((next, ele._2))
      ele._1
    }
  }
}
