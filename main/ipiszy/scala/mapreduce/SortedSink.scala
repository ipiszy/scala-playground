package ipiszy.scala.mapreduce

import java.util

/**
  * Created by carolzhang on 12/3/15.
  */
class SortedSink(val sink: Sink) extends Sink {
  private val map = new util.TreeMap[Array[Byte], Array[Byte]]()

  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    map.put(kv._1, kv._2)
  }

  override def close(): Unit = {
    val it = map.entrySet().iterator()
    while (it.hasNext) {
      val next = it.next()
      sink.writeKV((next.getKey, next.getValue))
    }
    sink.close()
  }
}
