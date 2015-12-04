package ipiszy.scala.mapreduce

import java.util
import java.util.Comparator

/**
  * Created by carolzhang on 12/3/15.
  */
class SortedSink(val sink: Sink) extends Sink {
  private val map = new util.TreeMap[Array[Byte], Array[Byte]](new Comparator[Array[Byte]] {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      // Copied from grepcode
      val len = Math.min(o1.length, o2.length);
      for (i <- 0 until len) {
        val diff = (o1(i) & 0xFF) - (o2(i) & 0xFF)
        if (diff != 0) return diff
      }
      return o1.length - o2.length
    }
  })

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
