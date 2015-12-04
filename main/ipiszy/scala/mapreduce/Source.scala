package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 11/30/15.
  */
trait Source {
  def readNextValue(): Array[Byte]
  def readNextKV(): (Array[Byte], Array[Byte])
}
