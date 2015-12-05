package ipiszy.scala.mapreduce

import com.google.common.hash.Hashing

/**
  * Created by carolzhang on 12/3/15.
  */
class HashedSink(val sinks: Array[Sink]) extends Sink {
  val hashFunc = Hashing.murmur3_128()
  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    sinks(hashFunc.hashBytes(kv._1).asInt().abs % sinks.length).writeKV(kv)
  }

  override def close(): Unit = {
    sinks.foreach(_.close())
  }
}
