package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 12/3/15.
  */
class HashedSink(val sinks: Array[Sink]) extends Sink {
  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    sinks(kv._1.hashCode() % sinks.length).writeKV(kv)
  }

  override def close(): Unit = {
    sinks.foreach(_.close())
  }
}
