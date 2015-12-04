package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 12/3/15.
  */
trait Mapper {
  def map(input: Array[Byte]): Array[(Array[Byte], Array[Byte])]
}
