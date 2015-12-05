package ipiszy.scala.mapreduce

import scala.collection.mutable.ArrayBuffer

/**
  * Created by carolzhang on 12/3/15.
  */
class SortedSink(val sink: Sink) extends Sink {
  private val arr = new ArrayBuffer[(Array[Byte], Array[Byte])]()

  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    arr += kv
  }

  override def close(): Unit = {
    val sortedArr = arr.sortBy(_._1.toIterable)
    sortedArr.foreach(sink.writeKV)
    sink.close()
  }
}
