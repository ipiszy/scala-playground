package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 12/3/15.
  */
trait Reducer {
  def reduce(key: Array[Byte], values: Array[Array[Byte]])
      : (Array[Byte], Array[Byte])
}
