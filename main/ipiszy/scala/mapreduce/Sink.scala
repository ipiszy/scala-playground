package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 11/30/15.
  */
trait Sink {
  def writeKV(key: Array[Byte], value: Array[Byte]): Unit
  def close(): Unit
}
